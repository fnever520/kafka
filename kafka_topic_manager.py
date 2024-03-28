from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaTopicWrapper():
    def __init__(self):
        self.admin_client = AdminClient({
            'bootstrap.servers': 'localhost:9092'
        })
    
    def create_topic_main(self, topic_name):
        existing_topics = self.admin_client.list_topics().topics

        for topic in existing_topics:
            if topic == topic_name:
                return "Exists"
            
        #Create new topic
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        self.admin_client.create_topics([new_topic])
        return "Created"
    
    def destroy_topic(self, topic_name):
        self.admin.delete_topic([topic_name])
        return "Deleted"
    

if __name__ == "__main__":
    kafka_topic = KafkaTopicWrapper()
    result = kafka_topic.create_topic_main(topic_name='fabian_topic')
    logger.info(result)