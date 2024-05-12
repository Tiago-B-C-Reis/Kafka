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
       bootstrap_servers=[kafka_brokker],
       value_serializer=lambda m: json.dumps(m).encode("ascii"),
   )

producer = connect_to_kafka()
# Create the topic `sensor-data` if it does not exist.
# <TODO: Add code to create the topic here>

# Cycle to publish data to Kafka
for x in range(0, num_messages):
   # Create
   new_message = get_new_record()
   # Print the new message
   print(new_message)

   # Publish to Kafka
   # <TODO: Complete the code here>

   # Wait for a few milliseconds
   time.sleep(0.6)

# be sure the messages are all sent
producer.flush()

