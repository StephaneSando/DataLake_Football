from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import psycopg2
import requests

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

BUCKET_RAW = "raw-football"
SEASONS = {
    "2023-24": "https://www.football-data.co.uk/mmz4281/2324/E0.csv",
    "2022-23": "https://www.football-data.co.uk/mmz4281/2223/E0.csv",
    "2021-22": "https://www.football-data.co.uk/mmz4281/2122/E0.csv",
    "2020-21": "https://www.football-data.co.uk/mmz4281/2021/E0.csv",
    "2019-20": "https://www.football-data.co.uk/mmz4281/1920/E0.csv",
}


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


def create_raw_bucket(**_):
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=BUCKET_RAW)
        logging.info("Bucket %s already exists", BUCKET_RAW)
    except Exception:
        client.create_bucket(Bucket=BUCKET_RAW)
        logging.info("Bucket %s created", BUCKET_RAW)


def log_raw_file(filename, source, bucket, object_key):
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO raw_files_log (filename, source, bucket, object_key)
            VALUES (%s, %s, %s, %s)
            """,
            (filename, source, bucket, object_key),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def download_and_upload_csv(season: str, url: str, **_):
    logging.info("Downloading %s from %s", season, url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    object_key = f"premier_league/{season}/matches.csv"
    get_minio_client().put_object(
        Bucket=BUCKET_RAW,
        Key=object_key,
        Body=response.content,
        ContentType="text/csv",
    )
    log_raw_file(f"{season}_matches.csv", "csv", BUCKET_RAW, object_key)
    nb_lines = len(response.text.strip().splitlines()) - 1
    logging.info("Season %s uploaded: %s matches", season, nb_lines)


with DAG(
    dag_id="dag_01_ingest_csv_football",
    default_args=default_args,
    description="Historical Premier League CSV ingestion to MinIO raw zone",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["raw", "football", "csv"],
) as dag:
    task_create_bucket = PythonOperator(
        task_id="create_raw_bucket",
        python_callable=create_raw_bucket,
    )

    upload_tasks = [
        PythonOperator(
            task_id=f"upload_{season.replace('-', '_')}",
            python_callable=download_and_upload_csv,
            op_kwargs={"season": season, "url": url},
        )
        for season, url in SEASONS.items()
    ]

    task_create_bucket >> upload_tasks
