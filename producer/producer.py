import os
import json
import time
import requests
import io
from google.cloud import pubsub_v1, secretmanager
from fastavro import parse_schema, schemaless_writer

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv("GCP_PROJECT_ID")
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")

# PROJECT_ID iz Secret Managera
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# PUBSUB_TOPIC iz Secret Managera
TOPIC_ID = get_secret("producer-pubsub-topic-secret")


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

with open("schema.avsc", "r") as f:
    avro_schema = parse_schema(json.load(f))

def fetch_posts():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()[:10]

def encode_avro(record):
    buffer = io.BytesIO()
    schemaless_writer(buffer, avro_schema, record)
    return buffer.getvalue()

def publish_posts(posts):
    for post in posts:
        avro_bytes = encode_avro(post)
        future = publisher.publish(topic_path, avro_bytes)
        print(f"Sent AVRO post {post['id']} to Pub/Sub")
        future.result()

if __name__ == "__main__":
    posts = fetch_posts()
    publish_posts(posts)

    while True:
        time.sleep(5)
