import time
import json
import argparse

from kafka import KafkaProducer
from faker import Faker
from faker.providers import internet, date_time, company, phone_number

from kfktest.util import (get_kafka_ssh, load_setup, linfo, gen_fake_data
)

# CLI 용 파서
parser = argparse.ArgumentParser(description="프로파일에 맞는 토픽에 레코드 생성.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('profile', type=str, help="프로파일 이름.")
parser.add_argument('-m', '--messages', type=int, default=10000, help="생성할 메시지 수.")
parser.add_argument('--acks', type=int, default=0, help="전송 완료에 필요한 승인 수.")
parser.add_argument('-c', '--compress', type=str,
    choices=['none', 'gzip', 'snappy', 'lz4'], default='none', help="데이터 압축 방식.")
parser.add_argument('-p', '--pid', type=int, default=0, help="셀렉트 프로세스 ID.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


def produce(profile,
        messages=parser.get_default('messages'),
        acks=parser.get_default('acks'),
        compress=parser.get_default('compress'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('dev')
        ):
    """Fake 레코드 전송.

    - 대상 토픽은 미리 존재하거나 브로커 설정에서 auto.create.topics.enable=true 여야 한다.

    """
    topic = f'{profile}-person'
    linfo(f"[ ] producer {pid} produces {messages} messages to {topic}.")

    setup = load_setup(profile)
    ip_key = 'kafka_public_ip' if dev else 'kafka_private_ip'
    broker_addr = setup[ip_key]['value']
    broker_port = 19092 if dev else 9092
    linfo(f"kafka broker at {broker_addr}:{broker_port}")
    compress = compress if compress != 'none' else None
    producer = KafkaProducer(
        acks=acks,
        compression_type=compress,
        bootstrap_servers=[f'{broker_addr}:{broker_port}'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    st = time.time()
    for i, data in enumerate(gen_fake_data(messages)):
        if (i + 1) % 500 == 0:
            linfo(f"gen {i + 1} th fake data")
        producer.send(topic, value=data)
    vel = messages / (time.time() - st)
    producer.flush()
    linfo(f"[ ] producer {pid} produces {messages} messages to {topic}. {int(vel)} rows per seconds.")


if __name__ == '__main__':
    args = parser.parse_args()
    produce(args.profile, args.messages, args.acks, args.compress, args.pid, args.dev)