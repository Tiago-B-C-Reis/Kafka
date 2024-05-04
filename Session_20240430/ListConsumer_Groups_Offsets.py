# Imports
from kafka import KafkaAdminClient

# configs
broker = "localhost:9092"
topic = "my-topic"

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

# {TopicPartition(topic=‘my-topic', partition=0): OffsetAndMetadata(offset=14, metadata=‘’) }
