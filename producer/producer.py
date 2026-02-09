import os
import json
import time
import requests
from google.cloud import pubsub_v1

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def fetch_posts():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()[:6]  # prvih 6 postova

def publish_posts(posts):
    for post in posts:
        data = json.dumps(post).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Sent post {post['id']} to Pub/Sub")
        future.result()

if __name__ == "__main__":
    posts = fetch_posts()
    publish_posts(posts)

    # beskonačna petlja (kao u uputama)
    while True:
        time.sleep(5)
