import os

import boto3
from fastapi import APIRouter
import psycopg2

router = APIRouter()


@router.get("/", summary="Service health check")
def health_check():
    status = {}
    try:
        conn = psycopg2.connect(
            host=os.environ["PG_HOST"],
            database=os.environ["PG_DB"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
            connect_timeout=5,
        )
        conn.close()
        status["postgresql"] = {"status": "healthy", "database": os.environ["PG_DB"]}
    except Exception as exc:
        status["postgresql"] = {"status": "unhealthy", "error": str(exc)}

    try:
        client = boto3.client(
            "s3",
            endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        )
        client.list_buckets()
        status["minio"] = {"status": "healthy", "endpoint": os.environ["MINIO_ENDPOINT"]}
    except Exception as exc:
        status["minio"] = {"status": "unhealthy", "error": str(exc)}

    all_healthy = all(service.get("status") == "healthy" for service in status.values())
    return {"overall": "healthy" if all_healthy else "degraded", "services": status}
