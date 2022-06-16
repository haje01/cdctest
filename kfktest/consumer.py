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
parser.add_argument('-t', '--timeout', type=int, default=10, help="타임아웃 시간(초)")
parser.add_argument('-a', '--auto-commit', action='store_true', default=False, help="오프셋 자동 커밋 여부")
parser.add_argument('-b', '--from-begin', action='store_true', default=False, help="처음부터 컨슘 여부")
parser.add_argument('-c', '--count-only', action='store_true', default=False, help="메시지 수만 카운팅")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


def consume(profile,
        timeout=parser.get_default('timeout'),
        auto_commit=parser.get_default('auto_commit'),
        from_begin=parser.get_default('from_begin'),
        count_only=parser.get_default('count_only'),
        dev=parser.get_default('dev')
        ):
    topic = f'my-topic-{profile}'
    print(f"Consume {topic}.")

    setup = load_setup(profile)
    ip_key = 'kafka_public_ip' if dev else 'kafka_private_ip'
    broker_addr = setup[ip_key]['value']
    broker_port = 19092 if dev else 9092

    consumer = KafkaConsumer(topic,
                    group_id=f'my-group-{profile}',
                    bootstrap_servers=[f'{broker_addr}:{broker_port}'],
                    auto_offset_reset='earliest' if from_begin else 'latest',
                    enable_auto_commit=auto_commit,
                    consumer_timeout_ms=timeout * 1000,
                    )

    print("Connected")
    cnt = 0
    for msg in consumer:
        cnt += 1
        import pdb; pdb.set_trace()
        if not count_only:
            print(f'{msg.topic}:{msg.partition}:{msg.offset} key={msg.key} value={msg.value}')

    print(f"Consume {cnt} messages.")
    return cnt


if __name__ == '__main__':
    args = parser.parse_args()
    consume(args.profile, args.timeout, args.auto_commit, args.from_begin, args.dev)