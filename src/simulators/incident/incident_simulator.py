import json
import time
import pandas as pd
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['redpanda-0:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

kafka_topic = 'incident_stream'
csv_file_path = 'accidents.csv'

print(f"Streaming raw NY records to Kafka topic: '{kafka_topic}'...")

# Fill NaN values with empty strings because json.dumps can't handle NaN values and will throw ValueError
df = pd.read_csv(csv_file_path).fillna('')  

try:
    for index, row in df.iterrows():
        # Convert the row to a dictionary
        row_dict = row.to_dict()
        
        # Push the exact raw row dictionary directly to Kafka
        producer.send(topic=kafka_topic, value=row_dict)
        print(f"Pushed Record ID: {row_dict.get('ID')}")
        
        # Strict 2-second delay
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStreaming stopped.")
finally:
    producer.flush()
    producer.close()