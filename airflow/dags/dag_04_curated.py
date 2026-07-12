from datetime import datetime, timedelta
from io import BytesIO
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import joblib
import pandas as pd
import psycopg2
import psycopg2.extras
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BUCKET_RAW = "raw-football"
FEATURE_COLUMNS = [
    "home_form",
    "away_form",
    "home_avg_goals_scored",
    "home_avg_goals_conceded",
    "away_avg_goals_scored",
    "away_avg_goals_conceded",
    "home_shots",
    "away_shots",
    "home_shots_on_target",
    "away_shots_on_target",
]
LABELS = ["A", "D", "H"]


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


def load_staging_dataframe():
    conn = get_pg_connection()
    try:
        df = pd.read_sql(
            """
            SELECT date, home_team, away_team, actual_result, result,
                   home_form, away_form, home_avg_goals_scored, home_avg_goals_conceded,
                   away_avg_goals_scored, away_avg_goals_conceded,
                   home_shots, away_shots, home_shots_on_target, away_shots_on_target
            FROM (
                SELECT date, home_team, away_team, result AS actual_result, result,
                       home_form, away_form, home_avg_goals_scored, home_avg_goals_conceded,
                       away_avg_goals_scored, away_avg_goals_conceded,
                       home_shots, away_shots, home_shots_on_target, away_shots_on_target
                FROM staging_matches
            ) s
            ORDER BY date
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        raise ValueError("staging_matches is empty. Run dag_03 first.")
    return df.dropna(subset=["actual_result"])


def train_xgboost(**_):
    df = load_staging_dataframe()
    if len(df) < 200:
        raise ValueError(f"Not enough data for training: {len(df)} matches")

    X = df[FEATURE_COLUMNS].fillna(0)
    label_encoder = LabelEncoder()
    label_encoder.fit(LABELS)
    y = label_encoder.transform(df["actual_result"])

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )
    model = XGBClassifier(
        objective="multi:softprob",
        num_class=3,
        n_estimators=120,
        max_depth=3,
        learning_rate=0.08,
        subsample=0.9,
        colsample_bytree=0.9,
        eval_metric="mlogloss",
        random_state=42,
    )
    model.fit(X_train, y_train)
    accuracy = accuracy_score(y_test, model.predict(X_test))
    model_version = datetime.utcnow().strftime("xgb_%Y%m%dT%H%M%SZ")

    artifact = {
        "model": model,
        "label_encoder": label_encoder,
        "feature_columns": FEATURE_COLUMNS,
        "accuracy": float(accuracy),
        "model_version": model_version,
    }
    buffer = BytesIO()
    joblib.dump(artifact, buffer)
    buffer.seek(0)
    get_minio_client().put_object(
        Bucket=BUCKET_RAW,
        Key=f"models/{model_version}.pkl",
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    logging.info("Model %s trained with accuracy %.4f", model_version, accuracy)


def load_latest_model():
    client = get_minio_client()
    response = client.list_objects_v2(Bucket=BUCKET_RAW, Prefix="models/")
    objects = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".pkl")]
    if not objects:
        raise ValueError("No model artifact found. Run train_xgboost first.")
    latest = sorted(objects, key=lambda item: item["LastModified"], reverse=True)[0]
    obj = client.get_object(Bucket=BUCKET_RAW, Key=latest["Key"])
    return joblib.load(BytesIO(obj["Body"].read()))


def generate_predictions(**_):
    df = load_staging_dataframe()
    artifact = load_latest_model()
    model = artifact["model"]
    label_encoder = artifact["label_encoder"]
    model_version = artifact["model_version"]

    X = df[artifact["feature_columns"]].fillna(0)
    predictions = model.predict(X)
    probabilities = model.predict_proba(X)
    predicted_labels = label_encoder.inverse_transform(predictions)
    class_to_index = {label: index for index, label in enumerate(label_encoder.classes_)}

    records = []
    for index, row in df.reset_index(drop=True).iterrows():
        probs = probabilities[index]
        records.append(
            {
                "date": row["date"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "actual_result": row["actual_result"],
                "predicted_result": predicted_labels[index],
                "prob_home": float(probs[class_to_index["H"]]),
                "prob_draw": float(probs[class_to_index["D"]]),
                "prob_away": float(probs[class_to_index["A"]]),
                "model_version": model_version,
            }
        )

    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE curated_predictions")
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO curated_predictions
                (date, home_team, away_team, actual_result, predicted_result,
                 prob_home, prob_draw, prob_away, model_version)
            VALUES
                (%(date)s, %(home_team)s, %(away_team)s, %(actual_result)s, %(predicted_result)s,
                 %(prob_home)s, %(prob_draw)s, %(prob_away)s, %(model_version)s)
            """,
            records,
            page_size=500,
        )
        conn.commit()
        logging.info("Inserted %s predictions with model %s", len(records), model_version)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="dag_04_curated_football",
    default_args=default_args,
    description="Train XGBoost and publish predictions to curated zone",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["curated", "football", "ml", "xgboost"],
) as dag:
    task_train = PythonOperator(task_id="train_xgboost", python_callable=train_xgboost)
    task_predict = PythonOperator(task_id="generate_predictions", python_callable=generate_predictions)

    task_train >> task_predict
