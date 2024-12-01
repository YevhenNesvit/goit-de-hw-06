from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = AdminClient({
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password']
})

topics = [
    "building_sensors_nesvit",
    "temperature_alerts_nesvit",
    "humidity_alerts_nesvit",
    "alerts_nesvit"
]

topic_list = [
    NewTopic(topics[0], num_partitions=3, replication_factor=1),
    NewTopic(topics[1], num_partitions=1, replication_factor=1),
    NewTopic(topics[2], num_partitions=1, replication_factor=1),
    NewTopic(topics[3], num_partitions=1, replication_factor=1)
]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Топіки створені успішно.")
except Exception as e:
    print(f"Помилка створення топіків: {e}")

# Отримуємо список топіків
cluster_metadata = admin_client.list_topics()

for topic in cluster_metadata.topics:
    if "nesvit" in topic:
        print(topic)
