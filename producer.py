from kafka import KafkaProducer

class Producer:
    def __init__(self, bootstrap_server: list, security_protocol: str, sasl_mechanism: str, username: str,password: str, topic: str):
        self.bootstrap_server = bootstrap_server
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.username = username
        self.password = password
        self.producer = None
    def init_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers= self.bootstrap_server,
            security_protocol= self.security_protocol,
            sasl_mechanism= self.sasl_mechanism,
            sasl_plain_username= self.username,
            sasl_plain_password= self.password,
            value_serializer=lambda x: x.encode('utf-8')
        )
    def push(self, topic, value, key = None):
        if self.producer is None:
            self.init_producer()
        self.producer.send(topic, value= value, key= key)
def main():
    bootstrap_server = ["localhost:9094", "localhost:9194", "localhost:9294"]
    topic = "my_topic"
    security_protocol = "SASL_PLAINTEXT"
    sasl_mechanism = "PLAIN"
    username = "admin"
    password = "Unigap@2024"
    producer = Producer(bootstrap_server, security_protocol, sasl_mechanism, username, password, topic)
    producer.push(topic, 'Hello')

if __name__ == "__main__":
    main()