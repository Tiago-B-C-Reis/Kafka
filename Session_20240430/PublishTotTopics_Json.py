# Imports
from kafka import KafkaProducer
import json


# Create producer
broker = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=[broker], value_serializer=lambda m: json.dumps(m).encode('ascii')) # Method 2 to encode the message

# Produce text
topic = "my-topic"
producer.send(topic, {'tech': 'kafka', 'rate': 100}) # here is where you can put your JSON, in this case is just entering json records directly
producer.flush()