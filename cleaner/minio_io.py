#minio_io.py

#This script handles acquiring the merged.csv from minio

import os
from minio import Minio
from minio.error import S3Error
import csv

from metrics import MINIO_ROWS_READ_TOTAL

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS   = os.getenv("MINIO_USER")
MINIO_SECRET   = os.getenv("MINIO_PASS")
MINIO_SECURE   = os.getenv("MINIO_SSL", "false").lower() == "true"

def get_minio_client():
    """Initialize and return a MinIO client using environment variables."""
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE
    )

def download_object(bucket: str, object_key: str, local_path: str):
    """Download an object (e.g. merged.csv) from MinIO to a local path."""
    client = get_minio_client()
    try:
        client.fget_object(bucket, object_key, local_path)
        print(f"[minio_io] Downloaded s3://{bucket}/{object_key} → {local_path}")

        # Count rows
        row_count = _count_rows(local_path)

        # Update Prometheus metric
        #MINIO_ROWS_READ_TOTAL.labels(
        #    bucket=bucket,
        #    object=object_key
        #).inc(row_count)# Count rows

        return row_count
    except S3Error as e:
        print(f"[minio_io] Failed to download {object_key}: {e}")
        raise


def _count_rows(file_path: str) -> int:
    """Count number of rows in a CSV file."""
    try:
        count = 0
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for _ in reader:
                count += 1
        print(f"[minio_io] {file_path} → {count} rows")
        return count
    except Exception as e:
        print(f"[minio_io] Failed to count rows: {e}")
        return 0