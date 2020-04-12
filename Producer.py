from kafka import KafkaProducer
from time import sleep
from json import dumps
import pandas as pd
from datetime import datetime

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)


producer = KafkaProducer(bootstrap_servers=['ec2-3-132-131-17.us-east-2.compute.amazonaws.com:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

df = pd.read_csv("twcs.csv", delimiter=",")

for index, row in df.iterrows():
    row['created_at'] = datetime.now()
    producer.send("accounts", row['author_id']).add_callback(on_send_success).add_callback(on_send_error)
    producer.send("tweets", row.to_json()).add_callback(on_send_success).add_callback(on_send_error)

producer.flush()
