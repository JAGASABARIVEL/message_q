
import json
from threading import Thread
from flask import  Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)


# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'  # Update with your Kafka broker URL
TOPIC = 'whatsapp'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON messages
)

def publish_message(topic, msg):
    producer.send(topic, msg)

@app.route("/msgq/ping")
def ping():
    return jsonify({'msg': 'Msg-q service is alive!'}), 200

@app.route("/msgq", methods=['POST'])
def send_msg():
    try:
        data = request.json
        message = data.get('message')
        publish_message(TOPIC, message)
        return jsonify({'status': 'Message sent successfully!'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5001, debug=True)
