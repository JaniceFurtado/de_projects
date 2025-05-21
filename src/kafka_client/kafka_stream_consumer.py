from kafka import KafkaConsumer
import time

# Connect to Kafka and subscribe to topic
consumer = KafkaConsumer(
    'sensorInfo',                           # Replace with your topic
    bootstrap_servers='localhost:9092',     # Match your Docker setup
    auto_offset_reset='earliest',           # Start from the beginning if no offset
    group_id='consumer-group',           # Consumer group ID
    enable_auto_commit=True,                # Commit offsets automatically
    value_deserializer=lambda x: x.decode('utf-8')  # Decode bytes to string
)

# Continuously listen for messages
def de_kafka_consumer():
    # List to store messages
    messages = []
    
    # Poll for messages during a time window (e.g., 10 seconds)
    timeout_secs = 10
    end_time = time.time() + timeout_secs
    
    print(f"Polling messages for {timeout_secs} seconds...")
    
    while time.time() < end_time:
        # Poll with short timeout to collect messages incrementally
        polled = consumer.poll(timeout_ms=10)
    
        for topic_partition, records in polled.items():
            for record in records:
                #print(f"Received: {record.value}")
                messages.append(record.value)

    print(f"Done. Collected {len(messages)} messages.")

    import json

    filepath="de_e2e/data/kafka_messages.json"
    with open(filepath, "w") as f:
        json.dump(messages, f, indent=2)
        
    print(f"Data written to file: ",{filepath})
    
if __name__ == '__main__':
    de_kafka_consumer()