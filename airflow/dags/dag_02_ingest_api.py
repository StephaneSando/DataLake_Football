from datetime import datetime, timedelta
import json
import logging
import os
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import psycopg2
import psycopg2.extras
import requests

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

BUCKET_RAW = "raw-football"
API_BASE = "https://api.football-data.org/v4"
COMPETITION = "PL"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        database=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


def ensure_bucket():
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=BUCKET_RAW)
    except Exception:
        client.create_bucket(Bucket=BUCKET_RAW)


def api_get(endpoint: str) -> dict:
    api_key = os.environ.get("FOOTBALL_API_KEY")
    if not api_key or api_key == "your_api_key_here":
        raise ValueError("FOOTBALL_API_KEY is missing in .env")
    response = requests.get(
        f"{API_BASE}/{endpoint}",
        headers={"X-Auth-Token": api_key},
        timeout=30,
    )
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", "60"))
        time.sleep(retry_after)
        response = requests.get(
            f"{API_BASE}/{endpoint}",
            headers={"X-Auth-Token": api_key},
            timeout=30,
        )
    response.raise_for_status()
    return response.json()


def upload_json_to_raw(payload: dict, object_key: str):
    ensure_bucket()
    get_minio_client().put_object(
        Bucket=BUCKET_RAW,
        Key=object_key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO raw_files_log (filename, source, bucket, object_key)
            VALUES (%s, %s, %s, %s)
            """,
            (object_key.split("/")[-1], "api", BUCKET_RAW, object_key),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def fetch_matches(**_):
    payload = api_get(f"competitions/{COMPETITION}/matches")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    upload_json_to_raw(payload, f"api/matches/{COMPETITION}_matches_{timestamp}.json")


def fetch_standings(**_):
    payload = api_get(f"competitions/{COMPETITION}/standings")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    upload_json_to_raw(payload, f"api/standings/{COMPETITION}_standings_{timestamp}.json")


def load_latest_api_matches_to_staging(**_):
    client = get_minio_client()
    response = client.list_objects_v2(Bucket=BUCKET_RAW, Prefix="api/matches/")
    objects = sorted(response.get("Contents", []), key=lambda item: item["LastModified"], reverse=True)
    if not objects:
        raise ValueError("No API match JSON found in MinIO")

    obj = client.get_object(Bucket=BUCKET_RAW, Key=objects[0]["Key"])
    payload = json.loads(obj["Body"].read())
    records = []
    for match in payload.get("matches", []):
        score = match.get("score", {}).get("fullTime", {})
        records.append(
            {
                "fixture_id": match.get("id"),
                "date": match.get("utcDate"),
                "home_team": match.get("homeTeam", {}).get("name"),
                "away_team": match.get("awayTeam", {}).get("name"),
                "home_score": score.get("home"),
                "away_score": score.get("away"),
                "status": match.get("status"),
                "competition": match.get("competition", {}).get("name"),
            }
        )

    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO staging_api_fixtures
                (fixture_id, date, home_team, away_team, home_score, away_score, status, competition)
            VALUES
                (%(fixture_id)s, %(date)s, %(home_team)s, %(away_team)s,
                 %(home_score)s, %(away_score)s, %(status)s, %(competition)s)
            ON CONFLICT (fixture_id) DO UPDATE SET
                date = EXCLUDED.date,
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                status = EXCLUDED.status,
                competition = EXCLUDED.competition,
                fetched_at = NOW()
            """,
            records,
            page_size=200,
        )
        conn.commit()
        logging.info("Loaded %s API fixtures into staging_api_fixtures", len(records))
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="dag_02_ingest_api_football",
    default_args=default_args,
    description="Daily football-data.org API ingestion to MinIO raw zone",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["raw", "football", "api"],
) as dag:
    task_fetch_matches = PythonOperator(task_id="fetch_matches", python_callable=fetch_matches)
    task_fetch_standings = PythonOperator(task_id="fetch_standings", python_callable=fetch_standings)
    task_load_fixtures = PythonOperator(
        task_id="load_latest_api_matches_to_staging",
        python_callable=load_latest_api_matches_to_staging,
    )

    [task_fetch_matches, task_fetch_standings] >> task_load_fixtures
