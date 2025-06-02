from kafka import KafkaProducer

class Producer:
    def __init__(self, config):
        self.bootstrap_server = config['bootstrap_server']
        self.security_protocol = config['security_protocol']
        self.sasl_mechanism = config['sasl_mechanism']
        self.username = config['username']
        self.password = config['password']
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