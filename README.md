# Kafka demo

This repo allows to easily run a local Kafka setup based on docker containers.
It includes some custom python script to simulate producers and consumers

## Installation
```
virtualenv -p python3 .env
pip3 install -r requirements.txt
```

## Cluster management
### Setup
```
docker-compose up -d
./update-hosts.sh  # in case you need to update /etc/hosts with the names of the services
```
### Finish cluster
```docker-compose down -v```

## Running producer script
```
cd demo/simple_app
TOPIC=<your_topic> LOCATION=<your_location_id> python3 producer
```

## Running producer script
```
cd demo/simple_app
TOPIC=<your_topic> LOCATION=<your_location_id> python3 consumer.py
```

## Running consumer script
```
cd demo/simple_app
TOPIC=<your_topic> python3 consumer.py
```

## Running schema update
```
cd demo/avro_app
TOPIC=<your_topic> python3 schema_uploader.py
```