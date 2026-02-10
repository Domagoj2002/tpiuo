import os
import json
import time
import requests
from google.cloud import pubsub_v1


def fetch_posts():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()[:6]


def publish_posts(posts, publisher, topic_path):
    for post in posts:
        data = json.dumps(post).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Sent post {post['id']} to Pub/Sub")
        future.result()


if __name__ == "__main__":

    # HARDKODIRANO ZA DEBUG
    PROJECT_ID = "student-0036540224-project"
    TOPIC_ID = "jsonplaceholder-topic"

    print("Loaded PROJECT_ID =", PROJECT_ID)
    print("Loaded TOPIC_ID =", TOPIC_ID)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    posts = fetch_posts()
    publish_posts(posts, publisher, topic_path)

    while True:
        time.sleep(5)
