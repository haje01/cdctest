import sys
import json
import argparse

from kafka import KafkaConsumer

from kfktest.util import load_setup

# CLI 용 파서
parser = argparse.ArgumentParser(description="프로파일에 맞는 토픽 컨슘.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('profile', type=str, help="프로파일 이름.")
parser.add_argument('-p', '--pid', type=int, default=0, help="셀렉트 프로세스 ID.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


args = parser.parse_args()
profile = args.profile
setup = load_setup(profile)

kafka_ip = setup['kafka_private_ip']['value']

consumer = KafkaConsumer(f'my_topic_{profile}',
                         group_id=f'my_group_{profile}',
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