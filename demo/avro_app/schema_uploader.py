import os
import json

import requests


KAFKA_TOPIC = os.getenv("TOPIC")
assert KAFKA_TOPIC is not None

headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

for element in ("key", "value"):

    # Read from the Avro schema file
    schema = open(f"condition_{element}.avsc", 'r').read()
    payload = {"schema": schema}

    url = f"http://schema-registry:8081/subjects/{KAFKA_TOPIC}-{element}/versions/"

    r = requests.post(url, data=json.dumps(payload), headers=headers)
    status = r.status_code
    if status == 200:
        print(f"Uploaded {element} schema")
    else:
        print(f"Failed upload for {element} schema: {r.text}")
