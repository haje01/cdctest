import time
import json
import argparse
import socket
from random import random

from confluent_kafka import Producer
# from kafka.errors import (KafkaConnectionError, KafkaTimeoutError,
#     UnknownTopicOrPartitionError)
from faker import Faker
from faker.providers import internet, date_time, company, phone_number

from kfktest.util import (get_kafka_ssh, load_setup, linfo, gen_fake_data
)

# CLI 용 파서
parser = argparse.ArgumentParser(description="프로파일에 맞는 토픽에 레코드 생성.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('profile', type=str, help="프로파일 이름 (. 이 있으면 도메인/IP 로 해석).")
parser.add_argument('-m', '--messages', type=int, default=10000, help="생성할 메시지 수.")
parser.add_argument('--acks', type=int, default=1, help="전송 완료에 필요한 승인 수.")
parser.add_argument('-c', '--compress', type=str,
    choices=['none', 'gzip', 'snappy', 'lz4'], default='none', help="데이터 압축 방식.")
parser.add_argument('-p', '--pid', type=int, default=0, help="셀렉트 프로세스 ID.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")
parser.add_argument('--lagrate', type=float, default=0, help="지연 메시지 비율.")
parser.add_argument('--lagdelay', type=int, default=60, help="지연 메시지 지연 시간(초).")
parser.add_argument('--duprate', type=float, default=0, help="중복 메시지 비율.")
parser.add_argument('--dupdelay', type=int, default=3, help="중복 메시지 지연 시간(초).")
parser.add_argument('-t', '--topic', type=str, default=None, help="명시적 토픽명")
parser.add_argument('--with_key', action='store_true', default=False, help="메시지 키 생성.")
parser.add_argument('--with_ts', action='store_true', default=False, help="메시지 타임스탬프 생성.")
parser.add_argument('--dt', type=str, default=None, help="지정된 일시로 메시지 생성.")

#
# 브로커가 없을 때 조용히 전송 메시지를 손실하는 문제
# https://github.com/dpkp/kafka-python/issues/2198
#
# class SafeKafkaProducer(KafkaProducer):
#     """메시지 전송이 실패하면 예외를 발생시키는 프로듀서.

#     - 브로커가 죽었을 때 조용히 전송 메시지를 손실하는 문제
#       https://github.com/dpkp/kafka-python/issues/2198
#     - 프로듀서에 retry 설정이 되어 있으면 재시도하는 듯
#       - 그렇지만 메시지는 유실되는 경우도 있고 아닌 경우도 있음

#     """

#     pending_futures = []

#     def send(self, *args, **kwargs):
#         future = super().send(*args, **kwargs)
#         self.pending_futures.append(future)
#         return future

#     def flush(self, timeout=None):
#         super().flush(timeout=timeout)
#         for future in self.pending_futures:
#             if future.failed():
#                 raise Exception("Failed to send batch, bailing out")
#         self.pending_futures = []


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    # else:
    #     print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def send(prod, topic, pid, _data, with_key):
    # Trigger any available delivery report callbacks from previous produce() calls
    prod.poll(0)
    data = json.dumps(_data).encode()
    if with_key:
        key = f"{pid}-{_data['id']}".encode()
        prod.produce(topic, data, key=key, callback=delivery_report)
    else:
        prod.produce(topic, data, callback=delivery_report)


def produce(profile,
        messages=parser.get_default('messages'),
        acks=parser.get_default('acks'),
        compress=parser.get_default('compress'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('dev'),
        lagrate=parser.get_default('lagrate'),
        lagdelay=parser.get_default('lagdelay'),
        duprate=parser.get_default('duprate'),
        dupdelay=parser.get_default('dupdelay'),
        etopic=parser.get_default('topic'),
        with_key=parser.get_default('with_key'),
        with_ts=parser.get_default('with_ts'),
        dt=parser.get_default('dt')
        ):
    """Fake 레코드 전송.

    - 대상 토픽은 미리 존재하거나 브로커 설정에서 auto.create.topics.enable=true 여야 한다.
    - linger_ms 가 있으면 전송 속도가 빨라진다.
    - 기본 구현은 브로커가 죽어도 전송시 예외 발생 않기에 SafeKafkaProducer 이용
    - 이따금씩 flush 를 명시적으로 불러주면 속도 많이 느려지지 않고 (~5%) 예외 확인 가능
        - 브로커 다운시에는 retry 탓인지 느려짐
        - retry 를 해도 메시지 손실이 발생할 수 있으나, 안하는 것보다는 작은 손실

    """
    if '.' not in profile:
        topic = f'{profile}_person' if etopic is None else etopic
        linfo(f"[ ] producer {pid} produces {messages} messages to {topic} with acks {acks}.")

        setup = load_setup(profile)
        ip_key = 'kafka_public_ip' if dev else 'kafka_private_ip'
        broker_addr = setup[ip_key]['value']
        broker_port = 19092 if dev else 9092
        addr = f'{broker_addr}:{broker_port}'
    else:
        topic = 'person' if etopic is None else etopic
        addr = profile
    linfo(f"kafka broker at {addr}")

    conf = {
        'bootstrap.servers': f'{addr}',
        'client.id': socket.gethostname(),
        'compression.codec': compress
        }
    prod = Producer(conf)
    # prod = SafeKafkaProducer(
    #     acks=acks,
    #     compression_type=compress,
    #     bootstrap_servers=[f'{broker_addr}:{broker_port}'],
    #     value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    #     )

    st = time.time()
    dup_msgs = []
    dup_times = []
    lag_msgs = []
    lag_times = []
    for i, data in enumerate(gen_fake_data(messages, with_ts)):
        if dt is not None:
            data['regdt'] = dt

        if (i + 1) % 500 == 0:
            linfo(f"gen {i + 1} th fake data")
            prod.flush()

        lagged = False
        if duprate > 0 and random() <= duprate:
            # 중복 메시지 발생
            dup_msgs.append(data)
            dup_times.append(time.time())
        elif lagrate > 0 and random() <= lagrate:
            # 지연 메시지 발생
            lag_msgs.append(data)
            lag_times.append(time.time())
            lagged = True
        if not lagged:
            send(prod, topic, pid, data, with_key)

        # 지연/중복 메시지 발행
        now = time.time()
        sents = []
        for i, msg in enumerate(lag_msgs):
            if now - lag_times[i] >= lagdelay:
                send(prod, topic, pid, data, with_key)
                sents.append(i)
        for i in sents:
            del lag_msgs[i]
            del lag_times[i]

        sents = []
        for i, msg in enumerate(dup_msgs):
            if now - dup_times[i] >= dupdelay:
                send(prod, topic, pid, data, with_key)
                sents.append(i)
        for i in sents:
            del dup_msgs[i]
            del dup_times[i]

        # 중복 / 지연이 설정된 경우는 메시지 천천히 발행 (순차적 시간 확인)
        if duprate > 0 or lagrate > 0:
            time.sleep(0.1)

    prod.flush()
    vel = messages / (time.time() - st)
    linfo(f"[v] producer {pid} produces {messages} messages to {topic}. {int(vel)} rows per seconds.")


if __name__ == '__main__':
    args = parser.parse_args()
    produce(args.profile, args.messages, args.acks, args.compress, args.pid,
        args.dev, args.lagrate, args.lagdelay, args.duprate, args.dupdelay,
        args.topic, args.with_key, args.with_ts, args.dt)
