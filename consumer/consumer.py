""" import base64
import json
from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST"])
def receive_message():
    envelope = request.get_json()

    # Logiraj cijeli envelope da vidiš što Pub/Sub šalje
    print("=== RAW ENVELOPE ===")
    print(envelope)

    if not envelope or "message" not in envelope:
        print("Envelope missing 'message' field")
        return "Bad Request", 400

    msg = envelope["message"]
    data = msg.get("data")

    if not data:
        print("Message received, but no 'data' field present")
        return "OK", 200

    try:
        decoded = base64.b64decode(data).decode("utf-8")
        print("=== DECODED DATA ===")
        print(decoded)

        post = json.loads(decoded)
        print(f"Received post {post['id']} - {post['title']}")

    except Exception as e:
        print("Error decoding or parsing message:", e)

    return "OK", 200

@app.route("/listening")
def listening():
    return "Consumer is running", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
 """
import base64
import json
import uuid
from flask import Flask, request, render_template_string
from google.cloud import storage

app = Flask(__name__)

received_posts = []

# Inicijaliziraj Cloud Storage klijenta
storage_client = storage.Client()
BUCKET_NAME = "jsonplaceholder-messages-domagoj"

def save_to_bucket(post):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"post-{post['id']}-{uuid.uuid4()}.json")
    blob.upload_from_string(json.dumps(post), content_type="application/json")
    print(f"Saved post {post['id']} to Cloud Storage")


@app.route("/", methods=["GET"])
def home():
    html = """
    <h1>Consumer Dashboard</h1>
    <p>Primljene Pub/Sub poruke:</p>
    <ul>
    {% for post in posts %}
        <li><strong>{{ post.id }}</strong>: {{ post.title }}</li>
    {% else %}
        <li><em>Još nema poruka...</em></li>
    {% endfor %}
    </ul>
    """
    return render_template_string(html, posts=received_posts)


@app.route("/", methods=["POST"])
def receive_message():
    envelope = request.get_json()
    print("=== RAW ENVELOPE ===")
    print(envelope)

    if not envelope or "message" not in envelope:
        return "Bad Request", 400

    msg = envelope["message"]
    data = msg.get("data")

    if not data:
        return "OK", 200

    try:
        decoded = base64.b64decode(data).decode("utf-8")
        post = json.loads(decoded)

        print(f"Received post {post['id']} - {post['title']}")

        # Spremi u memoriju
        received_posts.append(post)

        # Spremi u Cloud Storage
        save_to_bucket(post)

    except Exception as e:
        print("Error decoding or parsing message:", e)

    return "OK", 200


@app.route("/listening")
def listening():
    return "Consumer is running", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

