from distutils.ccompiler import get_default_compiler
from os import dup
import sys
import json
import argparse
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError, KafkaException

from kfktest.util import load_setup, linfo, DB_PRE_ROWS, DB_ROWS, \
    count_topic_message, SSH, consume_loop

# CLI 용 파서
parser = argparse.ArgumentParser(description="프로파일에 맞는 토픽 컨슘.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('profile', type=str, help="프로파일 이름.")
parser.add_argument('-g', '--cgid', type=str, default='kfkteste', help="컨슈머 그룹")
parser.add_argument('-t', '--timeout', type=int, default=5, help="타임아웃 시간(초)")
parser.add_argument('-a', '--auto-commit', action='store_true', default=False, help="오프셋 자동 커밋")
parser.add_argument('-b', '--from-begin', action='store_true', default=False, help="처음부터 컨슘")
parser.add_argument('-c', '--count-only', action='store_true', default=False, help="메시지 수만 카운팅")
parser.add_argument('-u', '--duplicate', action='store_true', default=False, help="중복 메시지 출력")
parser.add_argument('-m', '--miss', action='store_true', default=False, help="누락 메시지 ID 출력")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")
parser.add_argument('--topic', type=str, default=None, help="읽을 토픽 지정. (카운팅시 하나 이상 토픽을 ','로 구분해 지정 가능)")
parser.add_argument('-f', '--fields', type=str, default=None, help="일치하는 필드만 표시 (',' 로 구분).")
parser.add_argument('--field-types', type=str, default=None, help="일치하는 필드별 표시 타입 (',' 로 구분).")


def msg_process(msg, duplicate, miss, count_only, idmsgs, fields):
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()
    key = msg.key()
    value = msg.value()
    if duplicate or miss:
        data = json.loads(value.decode('utf8'))
        id = data['payload']['id']
        idmsgs[id].append((topic, partition, offset, data['payload']))
    else:
        if not count_only:
            if fields is None:
                linfo(f'{topic}:{partition}:{offset} key={key} value={value}')
            else:
                data = json.loads(value.decode('utf8'))
                values = []
                for f in fields.split(','):
                    field = f.strip()
                    if field in data['payload']:
                        values.append(data['payload'][field])
                linfo(f'{topic}:{partition}:{offset} key={key} value={values}')


def consume(profile,
        cgid=parser.get_default('cgid'),
        timeout=parser.get_default('timeout'),
        auto_commit=parser.get_default('auto_commit'),
        from_begin=parser.get_default('from_begin'),
        count_only=parser.get_default('count_only'),
        duplicate=parser.get_default('duplicate'),
        miss=parser.get_default('miss'),
        dev=parser.get_default('dev'),
        topic=parser.get_default('topic'),
        fields=parser.get_default('fields'),
        ):
    topic = f'{profile}_person' if topic is None else topic
    linfo(f"[ ] consume {topic}.")

    setup = load_setup(profile)
    ip_key = 'kafka_public_ip' if dev else 'kafka_private_ip'
    broker_addr = setup[ip_key]['value']
    broker_port = 19092 if dev else 9092
    if count_only:
        total = count_topic_message(profile, topic)
        linfo(f"[v] consume {topic} with {total} messages.")
        return
    else:
        assert ',' not in topic

    consumer = Consumer({
        'bootstrap.servers': f'{broker_addr}:{broker_port}',
        'group.id': cgid,
        'auto.offset.reset': 'earliest' if from_begin else 'latest',
        'enable.auto.commit': auto_commit,
        # 'session.timeout.ms': timeout * 1000
    })

    linfo("Connected")
    cnt = 0
    dup_cnt = 0
    idmsgs = defaultdict(list)
    consume_loop(consumer, [topic],
        lambda msg: msg_process(msg, duplicate, miss, count_only, idmsgs, fields),
        timeout)

    if duplicate:
        for id, msgs in idmsgs.items():
            dcnt = len(msgs) - 1
            if dcnt > 0:
                dup_cnt += dcnt
                linfo(f"msgid {id} has {dcnt} duplicate messages:")
                for msg in msgs:
                    linfo(f"   > {msg}")

    if miss:
        aids = set(range(1, DB_PRE_ROWS + DB_ROWS + 1))
        mids = set(idmsgs.keys())
        if len(aids) > len(mids):
            missed = aids - mids
            print(f"Missed message ids {missed} among 1 to {DB_PRE_ROWS + DB_ROWS}")


    linfo(f"[v] consume {topic} with {cnt} messages.")
    if duplicate:
        linfo(f"Total {dup_cnt} duplicate messages ({dup_cnt * 100/ float(cnt):.2f} %).")
    return cnt


if __name__ == '__main__':
    args = parser.parse_args()
    consume(args.profile, args.cgid, args.timeout, args.auto_commit, args.from_begin,
        args.count_only, args.duplicate, args.miss, args.dev, args.topic,
        args.fields)