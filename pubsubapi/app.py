import os
from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import subscriber
import json

project_id = "" 
topic_id = ""
subscription_id = ""
project_path = f"projects/{project_id}"

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_id, topic_id)

app = Flask(_name_)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


def pull_messages(subscription_id):
    print('subscription_id', subscription_id)
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    try:
        pull_response = subscriber.pull(subscription=subscription_path, max_messages=1, timeout=2.0)
    except Exception as e:
        return {}
    print('pull_response', pull_response)
    for msg in pull_response.received_messages:
        message = msg.message.data.decode('utf-8')
        subscriber.acknowledge(request={
            "subscription": subscription_path,
            "ack_ids": [msg.ack_id],
        })
        return {"message": message}
    return {}


@app.route("/")
@cross_origin()
def home():
    # name = os.environ.get("NAME", "World")
    return "Pub Sub API"

@app.route("/sendMessage", methods=["POST"])
@cross_origin()
def sendMessage():
    data = request.get_json()
    message = data["message"]
    topic = data["topic"]

    t_path = publisher.topic_path(project_id, topic)
    
    future = publisher.publish(t_path, json.dumps(message).encode("utf-8"))
    return jsonify({
        "status": "true",
        "message": "Message Sent" 
    })

@app.route("/receiveMessage", methods=["POST"])
@cross_origin()
def receiveMessage():
    data = request.get_json()
    subscription_id = data["subscriptionId"]
    message = pull_messages(subscription_id)
    print("message",message)
    return message


if _name_ == "_main_":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))