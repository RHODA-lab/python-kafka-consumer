import os
import json
from flask import Flask, jsonify, request
from confluent_kafka import Consumer, KafkaError

# Kafka data array
data = []

# Handler function for the HTTP GET request
application = Flask(__name__)
@application.route('/', methods=['GET'])
def get_data():
    return jsonify(data)

conf = {
    'bootstrap.servers': 'crdb-cluster-kafka-bootstrap.crdb-kafka.svc.cluster.local',
    'group.id': 'my-group-id',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['user7-table-changes'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached')
        else:
            print('Error while consuming message: {}'.format(msg.error()))
    else:
        print('Received message: {}'.format(msg.value().decode('utf-8')))
        # The kafka messages you receive on the topic are appended to the messages array
        # The contents of messages array can be accessed using an http GET
        data.append(msg.value().decode('utf-8'))
