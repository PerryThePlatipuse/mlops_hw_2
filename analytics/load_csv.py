import csv
import json
import sys

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda row: json.dumps(row).encode(),
)

with open(sys.argv[1], encoding='utf-8', newline='') as file:
    count = 0
    for row in csv.DictReader(file):
        producer.send('transactions_clickhouse', row)
        count += 1

producer.flush()
producer.close()

print(f'Sent {count} rows')
