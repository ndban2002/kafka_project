from producer import Producer
from consumer import Consumer
import configparser

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    producer = Producer(config['producer'])
    consumer = Consumer(config['consumer'])

    # Store consumer data to MongoDB
    consumer.read_to_mongodb()

    # Store consumer data to my Kafka's topic
    data = consumer.read()
    for value in data:
        producer.push(producer['topic'], value)

if __name__ == "__main__":
    main()