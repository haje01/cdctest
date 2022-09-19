import time
import json
import argparse

from kafka import KafkaProducer
from kafka.errors import (KafkaConnectionError, KafkaTimeoutError,
    UnknownTopicOrPartitionError)
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
parser.add_argument('--acks', type=int, default=1, help="전송 완료에 필요한 승인 수.")
parser.add_argument('-c', '--compress', type=str,
    choices=['none', 'gzip', 'snappy', 'lz4'], default='none', help="데이터 압축 방식.")
parser.add_argument('-p', '--pid', type=int, default=0, help="셀렉트 프로세스 ID.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")

#
# 브로커가 없을 때 조용히 전송 메시지를 손실하는 문제
# https://github.com/dpkp/kafka-python/issues/2198
#
class SafeKafkaProducer(KafkaProducer):
    """메시지 전송이 실패하면 예외를 발생시키는 프로듀서.

    - 브로커가 죽었을 때 조용히 전송 메시지를 손실하는 문제
      https://github.com/dpkp/kafka-python/issues/2198
    - 프로듀서에 retry 설정이 되어 있으면 재시도하는 듯
      - 그렇지만 메시지는 유실되는 경우도 있고 아닌 경우도 있음

    """

    pending_futures = []

    def send(self, *args, **kwargs):
        future = super().send(*args, **kwargs)
        self.pending_futures.append(future)
        return future

    def flush(self, timeout=None):
        super().flush(timeout=timeout)
        for future in self.pending_futures:
            if future.failed():
                raise Exception("Failed to send batch, bailing out")
        self.pending_futures = []


def produce(profile,
        messages=parser.get_default('messages'),
        acks=parser.get_default('acks'),
        compress=parser.get_default('compress'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('dev')
        ):
    """Fake 레코드 전송.

    - 대상 토픽은 미리 존재하거나 브로커 설정에서 auto.create.topics.enable=true 여야 한다.
    - linger_ms 가 있으면 전송 속도가 빨라진다.
    - 기본 구현은 브로커가 죽어도 전송시 예외 발생 않기에 SafeKafkaProducer 이용
    - 이따금씩 flush 를 명시적으로 불러주면 속도 많이 느려지지 않고 예외 확인 가능
        - 브로커 다운시에는 retry 탓인지 느려짐
        - retry 를 해도 메시지 손실이 발생할 수 있으나, 안하는 것보다는 작은 손실

    """
    topic = f'{profile}-person'
    linfo(f"[ ] producer {pid} produces {messages} messages to {topic} with acks {acks}.")

    setup = load_setup(profile)
    ip_key = 'kafka_public_ip' if dev else 'kafka_private_ip'
    broker_addr = setup[ip_key]['value']
    broker_port = 19092 if dev else 9092
    linfo(f"kafka broker at {broker_addr}:{broker_port}")
    compress = compress if compress != 'none' else None
    producer = SafeKafkaProducer(
        acks=acks,
        compression_type=compress,
        bootstrap_servers=[f'{broker_addr}:{broker_port}'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        # retry 값을 명시적으로 주어야 재시도
        retries=20,
        retry_backoff_ms=500,
        # 배치 전송으로 속도 향상
        linger_ms=100,
        )

    st = time.time()
    for i, data in enumerate(gen_fake_data(messages)):
        if (i + 1) % 500 == 0:
            linfo(f"gen {i + 1} th fake data")
            producer.flush()
        producer.send(topic, value=data)
    vel = messages / (time.time() - st)
    linfo(f"[v] producer {pid} produces {messages} messages to {topic}. {int(vel)} rows per seconds.")


if __name__ == '__main__':
    args = parser.parse_args()
    produce(args.profile, args.messages, args.acks, args.compress, args.pid, args.dev)