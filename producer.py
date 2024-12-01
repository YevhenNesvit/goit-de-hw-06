import random
import time
from confluent_kafka import Producer
import json
from datetime import datetime
from configs import kafka_config

# Створення продюсера Kafka
producer = Producer({
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password']
})

# Функція для створення випадкових даних
def generate_sensor_data(sensor_id):
    temperature = random.uniform(-300, 300)
    humidity = random.uniform(0, 100)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    data = {
        'sensor_id': sensor_id,
        'timestamp': timestamp,
        'temperature': temperature,
        'humidity': humidity
    }
    return data

# Відправка даних до Kafka
def send_data(sensor_id):
    while True:
        data = generate_sensor_data(sensor_id)
        producer.produce('building_sensors_nesvit', key=str(sensor_id), value=json.dumps(data))
        producer.flush()  # забезпечує негайну відправку
        print(f"Sent data: {data}")
        time.sleep(2)  # Затримка між відправками даних

# Запуск скрипту для кількох датчиків
for i in range(4):
    sensor_id = random.randint(1000, 9999)  # випадковий ID для датчика
    send_data(sensor_id)
