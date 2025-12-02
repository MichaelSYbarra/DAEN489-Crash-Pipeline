from minio import Minio
import os

MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_PASS", "minioadmin")

client = Minio(MINIO_URL, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)

def list_buckets():
    return [b.name for b in client.list_buckets()]

def list_prefix(bucket, prefix=""):
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects]

def delete_prefix(bucket, prefix):
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    for obj in objects:
        client.remove_object(bucket, obj.object_name)

def delete_bucket(bucket):
    # delete all objects first
    for obj in client.list_objects(bucket, recursive=True):
        client.remove_object(bucket, obj.object_name)
    client.remove_bucket(bucket)