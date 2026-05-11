from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

df = pd.read_csv("clickstream_large.csv")

for _, row in df.iterrows():
    producer.send("clickstream-topic", value=row.to_dict())
    time.sleep(0.005)  # controls streaming speed

print("Finished streaming dataset.")
