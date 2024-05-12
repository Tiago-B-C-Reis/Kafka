# Imports
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json


# Create producer
broker = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=[broker])

# Producing is asynchronous by default
topic = "my-topic"
future = producer.send(topic, b'hello world!') # Message encode 1: the 'b' is one way to encode the message to be sent

# If you want to wait for a message to be delivered, then
# block and wait (for 'synchronous' sends)
try:
    record_metadata = future.get(timeout=10)
    # Successful result returns assigned partition and offset
    print(f"Topic {record_metadata.topic}")
    print(f"Partition {record_metadata.partition}")
    print(f"Published offset {record_metadata.offset}")
except KafkaError:
    # Decide what to do if produce request failed...
    # log.exception()
    pass

# If you do not want to evaluate the produce message outcome,
# just force all the cached messages to be sent
producer.flush()