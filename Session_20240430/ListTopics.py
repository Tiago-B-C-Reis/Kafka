# Imports
from kafka.cluster import ClusterMetadata
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# Link to cluster
kmetadata = ClusterMetadata(bootstrap_servers=['localhost:9092'])

# Get nodeIds
print(kmetadata.brokers())