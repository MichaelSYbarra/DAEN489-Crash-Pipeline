import pika
import json
import time
import random
import traceback
import logging
from pika.exceptions import AMQPConnectionError, ProbableAccessDeniedError, ProbableAuthenticationError

import os

#Other files:
from minio_io import download_object
from duckdb_writer import write_to_duckdb
import sanity
from cleaning_rules import run_cleaning

from metrics import (
    CLEAN_MESSAGES_TOTAL,
    CLEANER_UPTIME_SECONDS,
    RABBIT_CONNECTIONS,
    MINIO_ROWS_READ_TOTAL,
    CLEANER_RUN_DURATION_SECONDS
)


#Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(filename)s] %(message)s'
)

from prometheus_client import Counter, Histogram, start_http_server

CLEAN_JOBS_TOTAL = Counter(
"clean_jobs_total",
"Total number of cleaning jobs processed",
["status"] # "success" or "failure"
)
CLEAN_JOB_DURATION_SECONDS = Histogram(
"clean_job_duration_seconds",
"Duration of cleaning jobs in seconds"
)


# --- Same variable naming as transformer ---
RABBIT_URL       = os.getenv("RABBITMQ_URL")
CLEAN_QUEUE      = os.getenv("CLEAN_QUEUE", "clean")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS     = os.getenv("MINIO_USER")
MINIO_SECRET     = os.getenv("MINIO_PASS")
MINIO_SECURE     = os.getenv("MINIO_SSL", "false").lower() == "true"
#CLEAN_BUCKET     = os.getenv("CLEAN_BUCKET", "gold")   # e.g. where cleaned data goes

def wait_for_port(host, port, tries=30, delay=1.0):
    import socket
    for i in range(tries):
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(delay)
    return False


def run_clean_job(msg):
    """Main cleaning entry point."""
    start_time = time.time()
    try:
        
        logging.info(f"[cleaner] Running clean job: {msg}")

        logging.info("[cleaner] Downloading from minIO")
        job_id = msg.get("job_id")
        bucket = msg.get("bucket")
        file_key = msg.get("file")

        # Wrap the process in a with so we can time it for the prometheus metric
        with CLEANER_RUN_DURATION_SECONDS.labels(bucket = bucket).time():

            local_path = "merged.csv"

            row_count = download_object(bucket, file_key, local_path)

            MINIO_ROWS_READ_TOTAL.labels(
                bucket=bucket,
            ).inc(row_count)# Count rows

            logging.info("[cleaner] Actually going to run the cleaning code")
            run_cleaning(local_path)

            logging.info("[Cleaner] Outputting file to duckdb")
            write_to_duckdb("cleaned.csv")

            logging.info("[Cleaner] Now running sanity checks")
            report = sanity.run_sanity_checks("/data/gold/gold.duckdb")
            sanity.log_sanity_report(report)

            logging.info(f"Cleaning complete for {file_key}")

            CLEAN_JOBS_TOTAL.labels(status="success").inc()

    except Exception as e:
        # Count failures at the job level
        CLEAN_JOBS_TOTAL.labels(status="failure").inc()
        logging.exception("[cleaner] Error while running clean job")
        # Re-raise so existing error handling in on_msg works as befor
    duration = time.time() - start_time
    CLEAN_JOB_DURATION_SECONDS.observe(duration)

"""
def run_clean_job(msg):
    #Main cleaning entry point.
    logging.info(f"[cleaner] Running clean job: {msg}")

    logging.info("[cleaner] Downloading from minIO")
    #Alright, downloading from minio
    job_id = msg.get("job_id")
    bucket = msg.get("bucket")
    file_key = msg.get("file")

    local_path = "merged.csv"

    download_object(bucket, file_key, local_path)

    logging.info("[cleaner] Actually going to run the cleaning code")

    run_cleaning(local_path)

    logging.info("[Cleaner] Outputing file to duckdb")

    write_to_duckdb("cleaned.csv")

    logging.info("[Cleaner] Now running sanity checks")

    report = sanity.run_sanity_checks("/data/gold/gold.duckdb")
    sanity.log_sanity_report(report)

    logging.info(f"Cleaning complete for {file_key}")
"""

def start_cleaner():
    params = pika.URLParameters(RABBIT_URL)

    host = params.host or "rabbitmq"
    port = params.port or 5672
    if not wait_for_port(host, port, tries=60, delay=1.0):
        raise SystemExit(f"[cleaner] RabbitMQ not reachable at {host}:{port}")

    conn = None
    for i in range(60):
        try:
            conn = pika.BlockingConnection(params)
            RABBIT_CONNECTIONS.inc()
            break
        except (AMQPConnectionError, ProbableAccessDeniedError, ProbableAuthenticationError):
            time.sleep(1.5 + random.random())

    if not conn or not conn.is_open:
        raise SystemExit("[cleaner] Could not connect to RabbitMQ.")

    ch = conn.channel()
    ch.queue_declare(queue=CLEAN_QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)

    def on_msg(chx, method, props, body):
        try:
            msg = json.loads(body.decode("utf-8"))
            if msg.get("type") != "clean":
                logging.info(f"Ignoring non-clean message: {msg}")
                CLEAN_MESSAGES_TOTAL.labels(result="ignored").inc()
                chx.basic_ack(delivery_tag=method.delivery_tag)
                return

            run_clean_job(msg)
            CLEAN_MESSAGES_TOTAL.labels(result="processed").inc()
            chx.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            CLEAN_MESSAGES_TOTAL.labels(result="failed").inc()
            traceback.print_exc()
            chx.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    logging.info(f"[cleaner] Waiting for clean jobs on queue '{CLEAN_QUEUE}'")
    ch.basic_consume(queue=CLEAN_QUEUE, on_message_callback=on_msg)

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
        conn.close()


if __name__ == "__main__":
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    logging.info(f"[cleaner] Starting Prometheus metrics server on port {metrics_port}")
    start_http_server(metrics_port)

    # Track uptime forever
    def update_uptime():
        start_time = time.time()
        while True:
            CLEANER_UPTIME_SECONDS.set(time.time() - start_time)
            time.sleep(5)

    import threading
    threading.Thread(target=update_uptime, daemon=True).start()


    start_cleaner()