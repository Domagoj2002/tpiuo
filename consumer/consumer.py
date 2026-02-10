import base64
import json
import io
from flask import Flask, request
from fastavro import parse_schema, schemaless_reader
from google.cloud import storage, bigquery
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Učitaj AVRO schemu
with open("reddit_schema.avsc", "r") as f:
    avro_schema = parse_schema(json.load(f))

# GCS
storage_client = storage.Client()
BUCKET = "jsonplaceholder-messages-domagoj"

# BigQuery
bq_client = bigquery.Client()
TABLE_ID = "student-0036540224-project.reddit_pipeline.reddit_messages"

def decode_avro(avro_bytes):
    buffer = io.BytesIO(avro_bytes)
    return schemaless_reader(buffer, avro_schema)

def save_json_to_gcs(record):
    ts = datetime.utcnow()
    path = f"json/year={ts.year}/month={ts.month}/day={ts.day}/post-{record['id']}.json"
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(record), content_type="application/json")
    print("Saved JSON:", path)
    return path

def save_parquet_to_gcs(record):
    df = pd.DataFrame([record])
    ts = datetime.utcnow()
    path = f"parquet/year={ts.year}/month={ts.month}/day={ts.day}/hour={ts.hour}/part-{record['id']}.parquet"
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(path)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    blob.upload_from_string(buffer.getvalue(), content_type="application/octet-stream")

    print("Saved Parquet:", path)
    return f"gs://{BUCKET}/{path}"

def load_to_bigquery(gcs_uri):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = bq_client.load_table_from_uri(gcs_uri, TABLE_ID, job_config=job_config)
    load_job.result()
    print("Loaded into BigQuery:", gcs_uri)

@app.route("/", methods=["POST"])
def receive():
    envelope = request.get_json()
    msg = envelope["message"]
    data = base64.b64decode(msg["data"])

    record = decode_avro(data)
    print("Decoded AVRO:", record)

    json_path = save_json_to_gcs(record)
    parquet_uri = save_parquet_to_gcs(record)
    load_to_bigquery(parquet_uri)

    return "OK", 200

@app.route("/")
def home():
    return "Consumer running", 200
