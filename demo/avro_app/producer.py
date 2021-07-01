"Python Avro Producer"

import sys
import os
import atexit
from time import sleep

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utils.temp_gen import temp_measures

KAFKA_TOPIC = os.getenv("TOPIC")
LOCATION_ID = os.getenv("LOCATION", "Munich")

assert KAFKA_TOPIC is not None

print("Starting Python Avro producer.")

value_schema = avro.load("condition_value.avsc")
key_schema = avro.load("condition_key.avsc")

# Configure the location of the bootstrap server, Confluent interceptors
# and a partitioner compatible with Java, and key/value schemas
# see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config = {
    'bootstrap.servers': 'kafka-1:19092,kafka-2:29092,kafka-3:39092',
    'plugin.library.paths': 'monitoring-interceptor',
    'partitioner': 'murmur2_random',
    'schema.registry.url': 'http://schema-registry:8081'
}
producer = AvroProducer(config, default_key_schema=key_schema, default_value_schema=value_schema)


def exit_handler():
    """Run this on exit"""
    print("Flushing producer and exiting.")
    producer.flush()


atexit.register(exit_handler)


def run_producer():
    temps = temp_measures()
    while True:
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        ts, temp_value = next(temps)
        key = {"key": LOCATION_ID}
        value = {
            "measure_ts": ts,
            "temperature": temp_value
        }

        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=value,
            callback=lambda err, msg:
                print(f"Sent Key: {key} Value: {value} [{msg.topic()} / {msg.partition()}]" if err is None else err)
            )
        sleep(5)


if __name__ == "__main__":
    run_producer()
