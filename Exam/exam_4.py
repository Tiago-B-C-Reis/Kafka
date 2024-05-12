# Imports
import time
import json
from datetime import datetime
from random import randrange, uniform
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# Function to simulate a new record
def get_new_record():
   sensors = ["tmp-int-1", "tmp-int-2", "tmp-ext-1", "tmp-ext-2"]
   selector = randrange(4)
   return {
       "stime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
       "sensor": sensors[selector],
       "data": (
           round(uniform(18, 20), 2)
           if selector in [0, 1]
           else round(uniform(9, 11), 2)
       ),
   }
# Number of messages to publish per script execution
num_messages = 60

# Connect to Kafka
kafka_broker = "localhost:9092"
kafka_topic = "sensor-data"


def connect_to_kafka():
   # Create the Kafka producer object
   return KafkaProducer(
       bootstrap_servers=[kafka_broker],
       value_serializer=lambda m: json.dumps(m).encode("ascii"),
   )

producer = connect_to_kafka()
# Create the topic `sensor-data` if it does not exist.
def create_topic_IFexists(kafka_broker, kafka_topic):
    consumer = KafkaConsumer(bootstrap_servers=[kafka_broker])
    for topic in consumer.topics():
        if kafka_topic and kafka_topic == topic:
            print(f"Topic '{topic}' already exists!")
            return True

# Connect using admin client
admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
# Create a new Topics list
new_topics_list = [NewTopic(name=kafka_topic, num_partitions=2, replication_factor=1)]

# Check if topic exists. Otherwise the command will fail
if not create_topic_IFexists(kafka_broker, kafka_topic):
    # Create the given topic
    print(f"Creating '{kafka_topic}' ...")
    admin_client.create_topics(new_topics=new_topics_list, validate_only=False)


# Cycle to publish data to Kafka
for x in range(0, num_messages):
   # Create
   new_message = get_new_record()
   # Print the new message
   print(new_message)

   # Publish to Kafka
   producer.send(kafka_topic, value=new_message)

   # Wait for a few milliseconds
   time.sleep(0.6)

# be sure the messages are all sent
producer.flush()

