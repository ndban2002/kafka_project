from kafka import KafkaConsumer

class Consumer:
    def __init__(self, bootstrap_server: list, security_protocol: str, sasl_mechanism: str, username: str, password: str, topic: str, consumer_group: str):
        self.bootstrap_server = bootstrap_server
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.username = username
        self.password = password
        self.topic = topic
        self.consumer_group = consumer_group
        self.consumer = None
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
def main():
    bootstrap_server = ["113.160.15.232:9094", "113.160.15.232:9194", "113.160.15.232:9294"]
    security_protocol = "SASL_PLAINTEXT"
    sasl_mechanism = "PLAIN"
    username = "kafka"
    password = "UnigapKafka@2024"
    topic = "product_view"
    consumer_group = "binhan-group"


    consumer = Consumer(bootstrap_server, security_protocol, sasl_mechanism, username, password, topic, consumer_group)
    data = consumer.read()
    for i, v in enumerate(data):
        print(v)
        if i > 5:
            break
if __name__ == "__main__":
    main()