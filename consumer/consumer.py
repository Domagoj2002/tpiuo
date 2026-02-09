import base64
import json
from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST"])
def receive_message():
    envelope = request.get_json()

    if not envelope or "message" not in envelope:
        return "Bad Request", 400

    msg = envelope["message"]
    data = msg.get("data")

    if data:
        decoded = base64.b64decode(data).decode("utf-8")
        post = json.loads(decoded)
        print(f"Received post {post['id']} - {post['title']}")

    return "OK", 200

@app.route("/listening")
def listening():
    return "Consumer is running", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
