"Python Consumer"

import sys
import os

from confluent_kafka import Consumer

KAFKA_TOPIC = os.getenv("TOPIC")
assert KAFKA_TOPIC is not None

print("Starting Python Consumer.")

# Configure the group id, location of the bootstrap server,
# Confluent interceptors
consumer = Consumer({
    'bootstrap.servers': 'kafka-1:19092,kafka-2:29092,kafka-3:39092',
    'plugin.library.paths': 'monitoring-interceptor',
    'group.id': 'demo-consumer',
    'auto.offset.reset': 'earliest',
    'fetch.wait.max.ms': 500,
    'fetch.min.bytes': 1
})

# Subscribe to our topic
topics = [t.strip() for t in KAFKA_TOPIC.split(',')]
consumer.subscribe(topics)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print("Key:{}\tValue:{}\t[{} / {} / {}]".format(
            msg.key().decode('utf-8'),
            msg.value().decode('utf-8'),
            msg.topic(),
            msg.partition(),
            msg.offset(),
        ))
except KeyboardInterrupt:
    sys.exit(1)
finally:
    # Clean up when the application exits or errors
    print("Closing consumer.")
    consumer.close()
