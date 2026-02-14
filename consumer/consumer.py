import base64
import json
import io
import os
from flask import Flask, request
from fastavro import parse_schema, schemaless_reader
from google.cloud import storage, bigquery
import pandas as pd
from datetime import datetime
from google.cloud import secretmanager

app = Flask(__name__)

# Učitaj AVRO schemu
with open("schema.avsc", "r") as f:
    avro_schema = parse_schema(json.load(f))

# GCS
storage_client = storage.Client()

# BigQuery
bq_client = bigquery.Client()

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv("GCP_PROJECT_ID")
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")

BUCKET = get_secret("consumer-bucket-secret")
TABLE_ID = get_secret("consumer-bqtable-secret")

# Za prikaz u browseru
last_message = None


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
    global last_message

    envelope = request.get_json()
    msg = envelope["message"]
    data = base64.b64decode(msg["data"])

    record = decode_avro(data)
    print("Decoded AVRO:", record)

    # spremi za prikaz u browseru
    last_message = record

    json_path = save_json_to_gcs(record)
    parquet_uri = save_parquet_to_gcs(record)
    load_to_bigquery(parquet_uri)

    return "OK", 200


@app.route("/")
def home():
    global last_message

    sql_example = """
SELECT *
FROM `student-0036540224-project.jsonplaceholder_pipeline.jsonplaceholder_messages`
ORDER BY id DESC
LIMIT 20;
"""

    if last_message:
        return (
            "<h2>Consumer running</h2>"
            "<h3>Last processed message:</h3>"
            f"<pre>{json.dumps(last_message, indent=2)}</pre>"
            "<h3>Run this SQL in BigQuery:</h3>"
            f"<pre>{sql_example}</pre>"
        )

    return (
        "<h2>Consumer running</h2>"
        "<p>No messages processed yet.</p>"
        "<h3>Run this SQL in BigQuery:</h3>"
        f"<pre>{sql_example}</pre>"
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
