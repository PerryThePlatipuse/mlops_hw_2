import json
import os

import psycopg2
from confluent_kafka import Consumer


connection = psycopg2.connect(os.environ['DATABASE_URL'])
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'score-writer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})
consumer.subscribe(['scores'])

try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(message.error())

        result = json.loads(message.value())
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'INSERT INTO scores (transaction_id, score, fraud_flag) '
                    'VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING',
                    (result['transaction_id'], result['score'], result['fraud_flag']),
                )
        consumer.commit(message=message, asynchronous=False)
finally:
    consumer.close()
    connection.close()
