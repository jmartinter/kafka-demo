"Python Simple Producer"

import sys
import os
import json
import atexit
from time import sleep

from confluent_kafka import Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utils.temp_gen import temp_measures


KAFKA_TOPIC = os.getenv("TOPIC")
LOCATION_ID = os.getenv("LOCATION", "Munich")

assert KAFKA_TOPIC is not None

print("Starting Python producer.")

# Configure the location of the bootstrap server, Confluent interceptors and a partitioner compatible with Java
# see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config = {
    'bootstrap.servers': 'kafka-1:19092,kafka-2:29092,kafka-3:39092',
    'plugin.library.paths': 'monitoring-interceptor',
    'partitioner': 'murmur2_random',
    'acks': 1,
    'linger.ms': 30000  # milliseconds
}
producer = Producer(config)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        m = f'Sent Key: {msg.key().decode()} Value: {msg.value().decode()} [{msg.topic()} / {msg.partition()}]'
        print(m)


def exit_handler():
    """Run this on exit"""
    print("Flushing pending messages and exiting.")
    producer.flush()


atexit.register(exit_handler)


def run_producer():
    temps = temp_measures()
    while True:
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        ts, temp_value = next(temps)
        value = {
            "measure_ts": ts,
            "temperature": temp_value
        }
        producer.produce(
            KAFKA_TOPIC,
            key=LOCATION_ID,
            value=json.dumps(value),
            on_delivery=delivery_report)
        sleep(5)


if __name__ == "__main__":
    run_producer()
