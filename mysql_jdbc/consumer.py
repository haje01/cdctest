import pdb
import sys
import json

from kafka import KafkaConsumer

assert len(sys.argv) == 2
setup = sys.argv[1]

with open(setup, 'rt') as f:
    setup = json.loads(f.read())

kafka_ip = setup['kafka_private_ip'].value

max_cnt = None
if len(sys.argv) == 2:
    max_cnt = int(sys.argv[1])

consumer1 = KafkaConsumer('my_topic_person',
                         group_id='my-group',
                         bootstrap_servers=[f'{kafka_ip}:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False
                         )

consumer2 = KafkaConsumer('my_topic_person',
                         group_id='my-group',
                         bootstrap_servers=[f'{kafka_ip}:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False
                         )

print("connected")
cnt = 0
for msg1, msg2 in zip(consumer1, consumer2):
    cnt += 1
    if max_cnt is not None:
        if cnt == max_cnt:
            break
    else:
        print(f'{msg.topic}:{msg.partition}:{msg.offset} key={msg.key} value={msg.value}')

if max_cnt is not None:
    print(cnt)