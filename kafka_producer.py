from confluent_kafka import Producer
import logging
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaProducerWrapper:
    def __init__(self):
        """
        Initialize the Kafka Producer with given bootstrap server.
        """
        bootstrap_server = 'localhost:9092'

        self.producer_config = {
            'bootstrap_server': bootstrap_server
        }

        self.producer = Producer(self.producer_config)

    def delivery_report(self, err, msg):
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    def produce_message(self, topic, key=None, value=None):
        """
        Produce message to the specified Kafka topic with the given key and value
        """
        self.producer.produce(topic, key=key, value=value, callback=self.delivery_report)
        self.producer.flush()

def kafka_producer_main(topic, key=None, value=None):
    kafka_producer = kafka_producer_main()
    start_time = time.time()

    try:
        while True:
            kafka_producer.produce_message(topic, key, value)
            logger.info("Produced message.")
            
            elapse_time = time.time() - start_time

            if elapse_time > 20:
                break
            
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Received keyboardInterrupt. Stopping producer")

    finally:
        kafka_producer.producer.flush()
        logger.info("Producer flushed.")

if __name__ == "__main__":
    kafka_producer_main()