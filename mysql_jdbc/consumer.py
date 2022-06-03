import sys

from kafka import KafkaConsumer

assert len(sys.argv) == 2
max_cnt = int(sys.argv[1])

consumer = KafkaConsumer('my_topic_person',
                         group_id='my-group3',
                         bootstrap_servers=['172.31.50.161:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False
                         )

print("connected")
cnt = 0
for msg in consumer:
    cnt += 1
    if cnt == max_cnt:
        break
    # print(f'{msg.topic}:{msg.partition}:{msg.offset} key={msg.key} value={msg.value}')

print(cnt)