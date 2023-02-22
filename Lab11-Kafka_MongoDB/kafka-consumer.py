# This Script acts as Kafka consumer. It pulls data from Kafka and save the data into MongoDB
# When pulls data from Kafka cluster, it uses confluent SDK
# When sends data to MongoDB, it uses pymongo SDK
import argparse
from confluent_kafka import Consumer
from typing import Type
from pymongo import MongoClient
import pymongo
import json

# function to connect to MongoDB
def create_mongodb_connection(host: str, username: str, password: str) -> Type[MongoClient]:
    try:
        client = MongoClient(
            host,
            27017,
            username=username,
            password=password,
            serverSelectionTimeoutMS=5)
        client.server_info()  # force connection on a request
        db = client.wiki
        print("Connected successfully!")
        return db

    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(f"Could not connect to MongoDB with following error {err}")


if __name__ == "__main__":
    # parse command line arguments
    parser = argparse.ArgumentParser(description='Kafka Consumer Script Arguments')

    parser.add_argument('--bootstrap_server', default='localhost:9092',
                        help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--topic_name', default='wikipedia',
                        help='Kafka topic name', type=str)
    parser.add_argument('--offset', default='earliest',
                        help='From where to start sonsuming messages', type=str)
    parser.add_argument('--mongodb_server', default='localhost',
                        help='MongoDB server', type=str)
    parser.add_argument('--mongodb_username', default='root',
                        help='MongoDB username to auth', type=str)
    parser.add_argument('--mongodb_password', default='password',
                        help='MongoDB password to auth', type=str)
    args = parser.parse_args()

    # init MongoDB connection
    connection = create_mongodb_connection(args.mongodb_server, args.mongodb_username, args.mongodb_password)

    # setup kafka consumer 
    consumerConfig = {
        'bootstrap.servers': args.bootstrap_server,
        'group.id': 'lab11Group',
        'auto.offset.reset': args.offset      
    }
    c = Consumer(consumerConfig)
    c.subscribe([args.topic_name])

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        msg = msg.value().decode('utf-8')
        connection.coba_info.insert_one(json.loads(msg))
        print(f"The following data is inserted in MongoDB database:\n{msg}")

