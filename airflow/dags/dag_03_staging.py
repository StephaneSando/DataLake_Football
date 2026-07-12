from collections import defaultdict, deque
from datetime import datetime, timedelta
from io import BytesIO
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
import psycopg2
import psycopg2.extras

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

BUCKET_RAW = "raw-football"
REQUIRED_COLUMNS = ["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HS", "AS", "HST", "AST"]


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


def parse_date_column(series):
    parsed = pd.to_datetime(series, dayfirst=True, errors="coerce")
    if parsed.isna().mean() > 0.5:
        parsed = pd.to_datetime(series, errors="coerce")
    return parsed


def load_csvs_from_raw(**_):
    client = get_minio_client()
    paginator = client.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=BUCKET_RAW, Prefix="premier_league/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            response = client.get_object(Bucket=BUCKET_RAW, Key=key)
            df = pd.read_csv(BytesIO(response["Body"].read()))
            missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
            if missing:
                raise ValueError(f"{key} missing columns: {missing}")
            df = df[REQUIRED_COLUMNS].copy()
            df["season"] = key.split("/")[1]
            df["source"] = "csv"
            frames.append(df)
            logging.info("Loaded %s with %s rows", key, len(df))

    if not frames:
        raise ValueError("No CSV files found in raw MinIO. Run dag_01 first.")
    combined = pd.concat(frames, ignore_index=True)
    combined.to_pickle("/tmp/football_raw_matches.pkl")


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "HomeTeam": "home_team",
            "AwayTeam": "away_team",
            "FTHG": "home_goals",
            "FTAG": "away_goals",
            "FTR": "result",
            "HS": "home_shots",
            "AS": "away_shots",
            "HST": "home_shots_on_target",
            "AST": "away_shots_on_target",
        }
    )
    df["date"] = parse_date_column(df["Date"])
    df = df.dropna(subset=["date", "home_team", "away_team", "result"])
    numeric_columns = [
        "home_goals",
        "away_goals",
        "home_shots",
        "away_shots",
        "home_shots_on_target",
        "away_shots_on_target",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)

    df = df.sort_values("date").reset_index(drop=True)
    history = defaultdict(lambda: deque(maxlen=5))
    records = []

    for _, row in df.iterrows():
        home_history = list(history[row["home_team"]])
        away_history = list(history[row["away_team"]])

        def form_points(items):
            return float(sum(item["points"] for item in items)) if items else 0.0

        def avg_for(items, key):
            return float(sum(item[key] for item in items) / len(items)) if items else 0.0

        record = {
            "date": row["date"].date(),
            "home_team": row["home_team"],
            "away_team": row["away_team"],
            "home_goals": int(row["home_goals"]),
            "away_goals": int(row["away_goals"]),
            "result": row["result"],
            "home_shots": int(row["home_shots"]),
            "away_shots": int(row["away_shots"]),
            "home_shots_on_target": int(row["home_shots_on_target"]),
            "away_shots_on_target": int(row["away_shots_on_target"]),
            "home_form": form_points(home_history),
            "away_form": form_points(away_history),
            "home_avg_goals_scored": avg_for(home_history, "goals_for"),
            "home_avg_goals_conceded": avg_for(home_history, "goals_against"),
            "away_avg_goals_scored": avg_for(away_history, "goals_for"),
            "away_avg_goals_conceded": avg_for(away_history, "goals_against"),
            "season": row["season"],
            "source": row["source"],
        }
        records.append(record)

        home_points = 3 if row["result"] == "H" else 1 if row["result"] == "D" else 0
        away_points = 3 if row["result"] == "A" else 1 if row["result"] == "D" else 0
        history[row["home_team"]].append(
            {"points": home_points, "goals_for": row["home_goals"], "goals_against": row["away_goals"]}
        )
        history[row["away_team"]].append(
            {"points": away_points, "goals_for": row["away_goals"], "goals_against": row["home_goals"]}
        )

    return pd.DataFrame(records)


def transform_and_load_staging(**_):
    raw_df = pd.read_pickle("/tmp/football_raw_matches.pkl")
    staging_df = build_features(raw_df)
    records = staging_df.to_dict("records")

    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE staging_matches")
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO staging_matches
                (date, home_team, away_team, home_goals, away_goals, result,
                 home_shots, away_shots, home_shots_on_target, away_shots_on_target,
                 home_form, away_form, home_avg_goals_scored, home_avg_goals_conceded,
                 away_avg_goals_scored, away_avg_goals_conceded, season, source)
            VALUES
                (%(date)s, %(home_team)s, %(away_team)s, %(home_goals)s, %(away_goals)s, %(result)s,
                 %(home_shots)s, %(away_shots)s, %(home_shots_on_target)s, %(away_shots_on_target)s,
                 %(home_form)s, %(away_form)s, %(home_avg_goals_scored)s, %(home_avg_goals_conceded)s,
                 %(away_avg_goals_scored)s, %(away_avg_goals_conceded)s, %(season)s, %(source)s)
            """,
            records,
            page_size=500,
        )
        conn.commit()
        logging.info("Inserted %s rows into staging_matches", len(records))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="dag_03_staging_football",
    default_args=default_args,
    description="Raw to staging transformation with rolling team-form features",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["staging", "football", "features"],
) as dag:
    task_load_csvs = PythonOperator(task_id="load_csvs_from_raw", python_callable=load_csvs_from_raw)
    task_transform_load = PythonOperator(
        task_id="transform_and_load_staging",
        python_callable=transform_and_load_staging,
    )

    task_load_csvs >> task_transform_load
