from confluent_kafka import Consumer, KafkaException, KafkaError
from configs import kafka_config

# Створення споживача Kafka
consumer = Consumer({
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password'],
    'group.id': 'alerts_group_nesvit',
    'auto.offset.reset': 'latest'
})

# Підписка на топік
consumer.subscribe(['alerts_nesvit'])

# Читання повідомлень
while True:
    msg = consumer.poll(timeout=1.0)  # Чекати 1 секунду
    if msg is None:
        print('No message available') # Немає нових повідомлень
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f'End of partition {msg.partition} reached at offset {msg.offset}')
        else:
            raise KafkaException(msg.error())
    else:
            print(f'Received message: {msg.value().decode("utf-8")}')
