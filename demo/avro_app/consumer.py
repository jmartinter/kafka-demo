"Python Avro Consumer"

import sys
import os

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

KAFKA_TOPIC = os.getenv("TOPIC")

print("Starting Python Avro Consumer.")

# Configure the group id, location of the bootstrap server,
# Confluent interceptors, and schema registry location
consumer = AvroConsumer({
    'bootstrap.servers': 'kafka-1:19092,kafka-2:29092,kafka-3:39092',
    'plugin.library.paths': 'monitoring-interceptor',
    'group.id': 'demo-consumer-avro',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://schema-registry:8081'
})

# Subscribe to our topic
consumer.subscribe([KAFKA_TOPIC])

try:
    while True:
        try:
            msg = consumer.poll(1.0)
        except SerializerError as ex:
            print(f"Message deserialization failed for {msg}: {ex}")
            break

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print("Key:{}\tValue:{}\t[{} / {} / {}]".format(
            msg.key(),
            msg.value(),
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
