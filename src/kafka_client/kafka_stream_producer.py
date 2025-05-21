from kafka import KafkaProducer
import json
import time
import random

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # broker IP
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate producing data
def generate_data():
    location_dict = {
    'ON': 'Ajax',
    'ON': 'Milton',
    'ON': 'Oakville',
    'ON': 'Whitby',
    'BC': 'Port Moody',
    'BC': 'Delta',
    'BC': 'Burnaby',
    'BC': 'Langley'
    }

    selected_key, selected_value = random.choice(list(location_dict.items()))
    
    data = {
        'timestamp': int(time.time()),
        'sensor_id': random.randint(1, 5),
        'value': random.random() * 100,
        'city': selected_value,
        'country': selected_key
    }
    return data

# Produce data to input-topic
def de_kafka_producer():
    count=0
    #while True:
    while count<100:
        data = generate_data()
        print(f"Sending: {data}")
        producer.send('input-topic', value=data)
        time.sleep(1)
        count +=1

    producer.flush()

if __name__ == '__main__':
    de_kafka_producer()