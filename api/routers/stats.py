import os

import boto3
from fastapi import APIRouter
import psycopg2
import psycopg2.extras

router = APIRouter()
BUCKET_RAW = "raw-football"


@router.get("/", summary="Data lake fill metrics")
def get_stats():
    result = {}
    try:
        client = boto3.client(
            "s3",
            endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        )
        objects = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_RAW):
            objects.extend(page.get("Contents", []))
        total_size = sum(obj["Size"] for obj in objects)
        result["raw"] = {
            "bucket": BUCKET_RAW,
            "nb_files": len(objects),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "csv_files": sum(1 for obj in objects if obj["Key"].endswith(".csv")),
            "json_files": sum(1 for obj in objects if obj["Key"].endswith(".json")),
            "model_files": sum(1 for obj in objects if obj["Key"].endswith(".pkl")),
        }
    except Exception as exc:
        result["raw"] = {"error": f"MinIO unavailable: {exc}"}

    try:
        conn = psycopg2.connect(
            host=os.environ["PG_HOST"],
            database=os.environ["PG_DB"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT COUNT(*) AS nb_matches,
                   COUNT(DISTINCT season) AS nb_seasons,
                   COUNT(DISTINCT home_team) AS nb_teams
            FROM staging_matches
            """
        )
        result["staging"] = dict(cur.fetchone())
        cur.execute(
            """
            SELECT COUNT(*) AS nb_predictions,
                   ROUND(
                       SUM(CASE WHEN actual_result = predicted_result THEN 1 ELSE 0 END)::numeric
                       / NULLIF(COUNT(*), 0), 4
                   ) AS model_accuracy,
                   MAX(model_version) AS latest_model_version
            FROM curated_predictions
            """
        )
        result["curated"] = dict(cur.fetchone())
        cur.close()
        conn.close()
    except Exception as exc:
        result["staging"] = {"error": str(exc)}
        result["curated"] = {"error": str(exc)}

    return {"data_lake": "Football Premier League", "zones": result}
