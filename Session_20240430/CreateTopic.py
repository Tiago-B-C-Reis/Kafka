# Imports
from kafka.cluster import ClusterMetadata
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

def topic_exists(broker, new_topic_name):
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    for topic in consumer.topics():
        if new_topic_name and new_topic_name == topic:
            print(f"Topic '{topic}' already exists!")
            return True
    return False

# Connect using admin client
broker = "localhost:9092"
admin_client = KafkaAdminClient(bootstrap_servers=broker)

# Create a new Topics list
topic_name = "my-topic"
new_topics_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]

# Check if topic exists. Otherwise the command will fail
if not topic_exists(broker, topic_name):
    # Create the given topic
    print(f"Creating '{topic_name}' ...")
    admin_client.create_topics(new_topics=new_topics_list, validate_only=False)