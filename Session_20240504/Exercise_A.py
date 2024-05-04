from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json


# 1. Verifies if a topic "players-data" exists. If it does not exist, creates it. Otherwise, prints a message saying the topic exists.
def topic_exists(broker, new_topic_name):
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    for topic in consumer.topics():
        if new_topic_name and new_topic_name == topic:
            print(f"Topic '{topic}' already exists!")
            return True
    # Connect using admin client
    admin_client = KafkaAdminClient(bootstrap_servers=broker)
    # Create a new Topics list
    new_topics_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]

    # Check if topic exists. Otherwise the command will fail
    if not topic_exists(broker, topic_name):
        # Create the given topic
        print(f"Creating '{topic_name}' ...")
        admin_client.create_topics(new_topics=new_topics_list, validate_only=False)
    return False


# 2. Publish the elements of the following collection (one by one) to they topic players-data.
def publish(broker):
    # Create producer
    producer = KafkaProducer(bootstrap_servers=[broker], value_serializer=lambda m: json.dumps(m).encode('ascii')) # Method 2 to encode the message
    # Produce text
    producer.send(topic_name, 
                [
                    { "_id": 1, "name": "Player1", "score": 520, "faults": 2 },
                    { "_id": 2, "name": "Player2", "score": 532, "faults": 0 },
                    { "_id": 3, "name": "Player3", "score": 498, "faults": 1 },
                    { "_id": 4, "name": "Player4", "score": 566, "faults": 4 },
                    ]
                ) # here is where you can put your JSON, in this case is just entering json records directly
    producer.flush()



# 3. Consume messages pending in the topic players-data. Use a consumer group id of your choice and print each message you read. 
# This consumer should close after 5 seconds without new messages.
# configs
def consume(broker, topic_name):
    group = "players-data-consumer-group_1" # radom name, it just needed to create a name for this consumer it self
    consumer_offset = "earliest"  # or "latest"
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(topic_name,
                            group_id=group,
                            bootstrap_servers=[broker],
                            consumer_timeout_ms=5000,  # consume for 5 seconds
                            auto_offset_reset=consumer_offset  # or "latest"
    )
    # iterate over each message in the consumer
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))



# 4. Using your preferred method (Kafka tools, Python, Conduktor, etc) find the offsets of your consumer
# configs
def offsets(broker):
    # set up a broker connection
    kafka = KafkaAdminClient(bootstrap_servers=broker)

    # get list of consumer groups
    active_consumers = kafka.list_consumer_groups()

    # iterate oveer consumers
    print("Active consumers:")

    for consumer in active_consumers:
        # Per consumer get the {topics:offsets} map
        print(f"\n - {consumer}")
        partition_mapping = kafka.list_consumer_group_offsets(group_id=consumer[0])

    # Per identified topic partition get the offsets
    for partition in partition_mapping:
        offsets = partition_mapping[partition]
        print(f" -- @ Topic:partition {partition.topic}:{partition.partition}")
        print(f" Offsets: {offsets}")
    
    



if __name__ == "__main__":
    
    # Connect using admin client
    broker = "localhost:9092"
    # Create a new Topics list
    topic_name = "players-data"
    
    # ex1:
    topic_exists(broker, topic_name)
    # ex2:
    publish(broker)
    # ex3:
    consume(broker, topic_name)
    # ex4:
    offsets(broker)
    
