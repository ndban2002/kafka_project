from kafka import KafkaConsumer
from pymongo import MongoClient, errors
import json

class Consumer:
    def __init__(self, config):
        self.bootstrap_server = config['bootstrap_server']
        self.security_protocol = config['security_protocol']
        self.sasl_mechanism = config['sasl_mechanism']
        self.username = config['username']
        self.password = config['password']
        self.topic = config['topic']
        self.consumer_group = config['consumer_group']
        self.consumer = None

        mongo_client = MongoClient(config['mongodb_uri'])
        self.mongo_collection = mongo_client[config['mongodb_db']][config['mongodb_collection']]
    def init_consumer(self):
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_server,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.username,
            sasl_plain_password=self.password,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.consumer_group,
            value_deserializer=lambda x: x.decode('utf-8')
        )
    def read(self):
        if self.consumer is None:
            self.init_consumer()
        for message in self.consumer:
            yield message.value

    def read_to_mongodb(self):
        if self.consumer is None:
            self.init_consumer()
        for count, message in enumerate(self.consumer):
            try:
                value = json.loads(message.value)
                try:
                    self.mongo_collection.insert_one(value)
                except errors.DuplicateKeyError:
                    continue
                if count % 10000 == 0:
                    print(f"Successfully storage {count} message to MongoDB")
            except Exception as err:
                raise err