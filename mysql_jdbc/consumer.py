import sys
import json

from kafka import KafkaConsumer

assert len(sys.argv) == 2
setup = sys.argv[1]

with open(setup, 'rt') as f:
    setup = json.loads(f.read())

kafka_ip = setup['kafka_private_ip']['value']

consumer = KafkaConsumer('my_topic_person',
                         group_id='my-group',
                         bootstrap_servers=[f'{kafka_ip}:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         consumer_timeout_ms=10000,
                         )

print("Connected")
cnt = 0
for msg in consumer:
    cnt += 1
    print(f'{msg.topic}:{msg.partition}:{msg.offset} key={msg.key} value={msg.value}')

print(f"Total: {cnt}")