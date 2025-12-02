import streamlit as st
from utils.minio_utils import list_buckets, list_prefix, delete_prefix, delete_bucket
from utils.duckdb_utils import get_gold_status, wipe_gold, sample_gold

from minio import Minio
from minio.error import S3Error
import os
import traceback

# --- MinIO Connection ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_PASS", "admin123")

try:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
except Exception as e:
    st.error(f"❌ Failed to connect to MinIO: {e}")
    st.stop()

st.header("📦 Data Management")

try:
    buckets = [b.name for b in client.list_buckets()]
except S3Error as e:
    st.error(f"❌ Failed to list buckets: {e}")
    buckets = []


# --- Bucket Deleter ---
if buckets:
    selected_bucket = st.selectbox("Select a bucket to delete:", buckets)

    # Confirmation step
    confirm_delete = st.checkbox(
        f"⚠️ I understand this will permanently delete all data in '{selected_bucket}'",
        value=False
    )

    delete_btn = st.button("Delete Bucket", type="primary", disabled=not confirm_delete)

    if delete_btn:
        try:
            # Before deleting the bucket, make sure it's empty
            objects = client.list_objects(selected_bucket, recursive=True)
            any_objects = any(True for _ in objects)

            if any_objects:
                st.warning(f"Bucket '{selected_bucket}' is not empty. Please empty it before deleting.")
            else:
                client.remove_bucket(selected_bucket)
                st.success(f"✅ Bucket '{selected_bucket}' deleted successfully!")
                st.rerun()

        except S3Error as e:
            st.error(f"❌ Failed to delete bucket: {e}")
else:
    st.info("No buckets found.")



# --- Bucket Selection ---
bucket_name = st.selectbox("Select Bucket", buckets)

# --- List Folders ---
def list_folders(bucket: str):
    try:
        objects = client.list_objects(bucket, recursive=False)
        folders = set()
        for obj in objects:
            if "/" in obj.object_name:
                prefix = obj.object_name.split("/")[0] + "/"
                folders.add(prefix)
        return sorted(folders)
    except Exception:
        st.error("❌ Failed to list folders.")
        st.text(traceback.format_exc())
        return []

folders = list_folders(bucket_name)

if folders:
    selected_folder = st.selectbox("Select Folder to Delete", folders)
else:
    st.info("No folders found in this bucket.")
    st.stop()

# --- Preview Contents ---
if st.button("Preview Folder Contents"):
    try:
        contents = client.list_objects(bucket_name, prefix=selected_folder, recursive=True)
        files = [obj.object_name for obj in contents]
        if files:
            st.write(f"Contents of `{selected_folder}`:")
            st.dataframe(files)
        else:
            st.info("Folder is empty.")
    except Exception:
        st.error("❌ Failed to preview folder contents.")
        st.text(traceback.format_exc())

# --- Delete Folder ---
confirm_delete = st.checkbox("⚠️ I understand this will permanently delete all data in this folder")
if st.button("Delete Folder") and confirm_delete:
    try:
        contents = client.list_objects(bucket_name, prefix=selected_folder, recursive=True)
        for obj in contents:
            client.remove_object(bucket_name, obj.object_name)
        st.success(f"✅ Deleted all objects under `{selected_folder}`")
    except Exception:
        st.error("❌ Failed to delete folder.")
        st.text(traceback.format_exc())

# ---- B) GOLD ADMIN ----
st.header("🏆 Gold (DuckDB) Admin")

status = get_gold_status()
if not status["exists"]:
    st.warning("Gold DB not found.")
else:
    st.info(f"Total rows: {status['rows']}")
    st.write(status["tables"])

if st.checkbox("⚠️ I understand this will permanently delete all data in the gold database"):
    if st.button("Wipe Gold DB"):
        wipe_gold()
        st.success("Gold DB wiped and reset.")

# ---- C) QUICK PEEK ----
st.header("🔍 Quick Peek (Gold Table Sample)")

if status["exists"]:
    cols = list(status["tables"].keys())
    df = sample_gold(limit=10)
    if not df.empty:
        selected_cols = st.multiselect("Select columns", df.columns.tolist(), default=df.columns[:6])
        limit = st.slider("Rows", 10, 200, 50)
        if st.button("Preview Gold"):
            st.dataframe(sample_gold(selected_cols, limit))
    else:
        st.warning("No data in Gold DB yet.")