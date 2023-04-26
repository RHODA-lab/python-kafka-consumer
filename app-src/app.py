# This code works like a charm for a developer user who is in their own namespace
#
import os
from confluent_kafka import Consumer, KafkaError
import requests

# Kafka cluster config
conf = {
    'bootstrap.servers': 'crdb-cluster-kafka-bootstrap.crdb-kafka.svc.cluster.local',
    'group.id': 'my-group-id',
    'auto.offset.reset': 'earliest'
}

# Initialize Consumer and subscribe to kafka topic
consumer = Consumer(conf)
#consumer.subscribe(['user7-table-changes'])
topic_name = os.getenv('TOPIC_NAME')
print("Topic name is set to: ", topic_name)
consumer.subscribe([topic_name])

def main():
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

if __name__ == "__main__":
    main()
