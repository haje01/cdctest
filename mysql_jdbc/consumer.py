import sys

from kafka import KafkaConsumer

max_cnt = None
if len(sys.argv) == 2:
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
    if max_cnt is not None:
        if cnt == max_cnt:
            break
    else:
        print(f'{msg.topic}:{msg.partition}:{msg.offset} key={msg.key} value={msg.value}')

if max_cnt is not None:
    print(cnt)