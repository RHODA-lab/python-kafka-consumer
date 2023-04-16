import os
import json
from flask import Flask, jsonify, request
from confluent_kafka import Consumer, Producer, KafkaError

application = Flask(__name__)

# Create a Kafka producer configuration object
bootstrap_servers = "crdb-cluster-kafka-bootstrap.crdb-kafka.svc.cluster.local"
conf = {"bootstrap.servers": bootstrap_servers}

# Initialize Consumer and subscribe to kafka topic
producer = Producer(conf)
topic = "user7-table-changes"

# Define the callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@application.route('/')
@application.route('/produce')
def kafka_produce():
    for i in range(20):
        # Construct the message to be produced
        message = f"Kafka Test Message {i}".encode("utf-8")
        # Use the producer instance to produce the message to the topic
        producer.produce(topic, key=str(i), value=message, callback=delivery_report)
        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()
    return jsonify({'kafka_produce': 'posted kafka messages'})


@application.route('/status')
def status():
    return jsonify({'status': 'kafka consumer is running'})

