from typing import Optional
import json
import os

import boto3
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()
BUCKET_RAW = "raw-football"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


@router.get("/", summary="List raw files in MinIO")
def list_raw_files(
    prefix: Optional[str] = Query(None, description="Prefix filter, for example api/matches/"),
    limit: int = Query(50, le=500),
):
    client = get_minio_client()
    try:
        kwargs = {"Bucket": BUCKET_RAW, "MaxKeys": limit}
        if prefix:
            kwargs["Prefix"] = prefix
        response = client.list_objects_v2(**kwargs)
        files = response.get("Contents", [])
        return {
            "zone": "raw",
            "bucket": BUCKET_RAW,
            "count": len(files),
            "files": [
                {
                    "key": file["Key"],
                    "size_kb": round(file["Size"] / 1024, 2),
                    "last_modified": file["LastModified"].isoformat(),
                }
                for file in files
            ],
        }
    except client.exceptions.NoSuchBucket:
        raise HTTPException(status_code=404, detail=f"Bucket {BUCKET_RAW} not found")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MinIO error: {exc}")


@router.get("/file", summary="Get a raw JSON file")
def get_raw_file(key: str = Query(..., description="Object key in MinIO")):
    if not key.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only JSON files are exposed by this endpoint")
    client = get_minio_client()
    try:
        obj = client.get_object(Bucket=BUCKET_RAW, Key=key)
        return {"key": key, "content": json.loads(obj["Body"].read())}
    except client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"Object {key} not found")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Read error: {exc}")
