from producer import Producer
from consumer import Consumer

def main():
    consumer_config = {
        "bootstrap_server": ["113.160.15.232:9094", "113.160.15.232:9194", "113.160.15.232:9294"],
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "username": "kafka",
        "password": "UnigapKafka@2024",
        "topic": "product_view",
        "consumer_group": "binhan-group",
        "mongodb_uri": "localhost:27017",
        "mongodb_db": "kafka_project",
        "mongodb_collection": "streaming_data"
    }

    producer_config = {
        "bootstrap_server": ["localhost:9094", "localhost:9194", "localhost:9294"],
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "username": "admin",
        "password": "Unigap@2024",
        "topic": "kafka_project_topic",
    }

    producer = Producer(producer_config)
    consumer = Consumer(consumer_config)

    # Store consumer data to MongoDB
    # consumer.read_to_mongodb()

    # Store consumer data to my Kafka's topic
    data = consumer.read()
    for value in data:
        producer.push(producer_config['topic'], value)

if __name__ == "__main__":
    main()