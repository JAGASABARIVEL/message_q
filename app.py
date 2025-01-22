import json
from flask import Flask, request, jsonify
from confluent_kafka import Producer, KafkaError

app = Flask(__name__)

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'  # Update with your Kafka broker URL
TOPIC = 'whatsapp'

# Initialize Kafka Producer
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,  # Confluent Kafka broker URL
    'client.id': 'flask-app',          # Optional client identifier
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """
    Callback for delivery reports.
    Called once for each message produced to indicate delivery result.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def publish_message(topic, msg):
    try:
        # Produce a message asynchronously
        producer.produce(
            topic,
            value=json.dumps(msg),
            callback=delivery_report  # Attach the delivery callback
        )
        # Ensure messages are flushed to Kafka
        producer.flush()
    except KafkaError as e:
        print(f"Failed to produce message: {e}")

@app.route("/msgq/ping")
def ping():
    return jsonify({'msg': 'Msg-q service is alive!'}), 200

@app.route("/msgq", methods=['POST'])
def send_msg():
    try:
        data = request.json
        message = data.get('message')
        if not message:
            raise ValueError("Message is required in the request body.")
        publish_message(TOPIC, message)
        return jsonify({'status': 'Message sent successfully!'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5001, debug=True)
