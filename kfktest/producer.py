import sys
import json
import argparse

from kafka import KafkaProducer
from faker import Faker
from faker.providers import internet, date_time, company, phone_number

from kfktest.util import load_setup, linfo

# CLI 용 파서
parser = argparse.ArgumentParser(description="프로파일에 맞는 토픽에 레코드 생성.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('profile', type=str, help="프로파일 이름.")
parser.add_argument('-c', '--count', type=int, default=10000, help="생성할 레코드 수.")
parser.add_argument('--acks', type=int, default=0, help="전송 완료에 필요한 승인 수.")
parser.add_argument('--compress', type=str,
    choices=['none', 'gzip', 'snappy', 'lz4'], default='none', help="데이터 압축 방식.")
parser.add_argument('--callback', action='store_true', default=False, help="Ack 결과 콜백.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


def produce(profile,
        count=parser.get_default('count'),
        acks=parser.get_default('acks'),
        compress=parser.get_default('compress'),
        dev=parser.get_default('dev')
        ):
    """Fake 레코드 생성.

    Dev 모드로 로컬 PC 에서 Kafka 접근하려면 server.properties 에서 Public IP 로 수정 필요.

    """
    topic = f'{profile}-person'
    linfo(f"Produce {count} records to {topic}.")

    setup = load_setup(profile)
    ip_key = 'kafka_public_ip' if dev else 'kafka_private_ip'
    broker_addr = setup[ip_key]['value']
    broker_port = 19092 if dev else 9092
    compress = compress if compress != 'none' else None
    producer = KafkaProducer(
        acks=acks,
        compression_type=compress,
        bootstrap_servers=[f'{broker_addr}:{broker_port}'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    fake = Faker()
    fake.add_provider(internet)
    fake.add_provider(date_time)
    fake.add_provider(company)
    fake.add_provider(phone_number)

    for i in range(count):
        data = {
            'id': i + 1,
            'name': fake.name(),
            'address': fake.address(),
            'ip': fake.ipv4_public(),
            'birth': fake.date(),
            'company': fake.company(),
            'phone': fake.phone_number()
        }
        producer.send(topic, value=data)

    producer.flush()
    linfo("Produce done.")


if __name__ == '__main__':
    args = parser.parse_args()
    produce(args.profile, args.count, args.acks, args.compress, args.dev)