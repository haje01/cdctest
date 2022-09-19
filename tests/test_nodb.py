import time
import json
from multiprocessing import Process, Queue

import pytest
from kafka import KafkaProducer, KafkaConsumer

from kfktest.util import (local_produce_proc, local_consume_proc, local_exec,
    linfo, remote_produce_proc, count_topic_message, s3_count_sinkmsg,
    KFKTEST_S3_BUCKET, KFKTEST_S3_DIR, unregister_kconn, register_s3sink,
    load_setup, _hash, kill_proc_by_port, start_kafka_broker, ssh_exec,
    # 픽스쳐들
    xsetup, xtopic, xkfssh, xkvmstart, xcp_setup, xs3sink, xhash, xs3rmdir,
    xrmcons, xconn, xkafka, xzookeeper
)
from kfktest.producer import produce
from kfktest.consumer import consume

NUM_PRO_PROCS = 4
PROC_NUM_MSG = 10000


@pytest.fixture(scope="session")
def xprofile():
    return 'nodb'


def test_local_basic(xkafka, xprofile, xsetup, xtopic, xkfssh):
    """로컬 프로듀서 및 컨슈머로 기본 동작 테스트."""
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, PROC_NUM_MSG))
        p.start()
        pro_pros.append(p)

    for p in pro_pros:
        p.join()

    # 메시지 수집 대기
    time.sleep(3)
    cnt = count_topic_message(xprofile, xtopic)
    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. {int(vel)} rows per seconds.")
    assert tot_msg == cnt


def test_local_basic_brk(xkafka, xprofile, xsetup, xtopic, xkfssh):
    """브로커가 죽을 때 로컬 프로듀서 및 컨슈머로 동작 테스트.

    - 브로커가 죽으면 메시지 손실이 발생할 수 있음 (아닌 경우도 있음)

    """
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, PROC_NUM_MSG))
        p.start()
        pro_pros.append(p)

    # 잠시 후 카프카 브로커 강제 종료
    time.sleep(2)
    kill_proc_by_port(xkfssh, 9092)
    # 잠시 후 카프카 브로커 start
    time.sleep(2)
    start_kafka_broker(xkfssh)

    for p in pro_pros:
        p.join()

    # 메시지 수집 대기
    time.sleep(5)
    cnt = count_topic_message(xprofile, xtopic)
    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. topic has {cnt} messages.")
    assert tot_msg >= cnt


def test_remote_basic(xkafka, xprofile, xsetup, xcp_setup, xtopic, xkfssh):
    """원격 프로듀서 및 컨슈머로 기본 동작 테스트."""
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=remote_produce_proc, args=(xprofile, xsetup, pid, PROC_NUM_MSG))
        p.start()
        pro_pros.append(p)

    for p in pro_pros:
        p.join()

    time.sleep(3)
    cnt = count_topic_message(xprofile, xtopic)
    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. {int(vel)} rows per seconds.")
    assert tot_msg == cnt


@pytest.mark.parametrize('xtopic', [{
    'partitions': 1,
    'topic_cfg': {
        # 5초 마다 로그 컴팩션 하도록 토픽 생성
        'cleanup.policy': 'compact',
        # 1초 마다 세그먼트가 닫힘
        'segment.ms': 1000,
        # dirty ratio: 헤드 세그먼트의 바이트 / 전체 (헤드 + 테일) 세그먼트의 바이트가 이값보다 크면 클리닝(컴팩션) 시작
        'min.cleanable.dirty.ratio': 0.001  # 항상 수행되게
        }
     }], indirect=True)
def test_log_comp(xprofile, xcp_setup, xtopic):
    """로그 컴팩션 테스트.

    - 로그 컴팩션은 카프카의 효율을 위한 것이지, 중복 메시지 제거를 위한 것이 아님
      https://stackoverflow.com/questions/61552299/is-kafka-log-compaction-also-a-de-duplication-mechanism
    - 같은 키의 메시지가 하나의 여러 세그먼트에 존재하면 중복 메시지 발생 가능

    """
    setup = load_setup(xprofile)
    broker = f"{setup['kafka_public_ip']['value']}:19092"
    # 메시지 생성
    prod = KafkaProducer(
        bootstrap_servers=broker
        )
    prod.send(xtopic, b'100', b'Bob')
    prod.send(xtopic, b'100', b'Lucy')
    prod.flush()
    time.sleep(1)  # 세그먼트 종료 대기

    prod.send(xtopic, b'200', b'Bob')
    prod.send(xtopic, b'200', b'Lucy')
    prod.send(xtopic, b'200', b'Patric')
    prod.flush()
    time.sleep(1)  # 세그먼트 종료 대기

    # 로그 컴팩션은 헤드 세그먼트가 존재할 때 종료된 세그먼트들에 대해 수행되고
    # 컴팩션 결과 종료된 세그먼트들은 지워지고 하나의 테일 세그만트만 남는다.

    prod.send(xtopic, b'300', b'Patric')
    prod.flush()

    # 로그 컴팩션 완료를 기다린 후 결과 확인
    time.sleep(13)
    cons = KafkaConsumer(xtopic,
        bootstrap_servers=broker,
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        key_deserializer=decoder,
        value_deserializer=decoder
    )

    # 로그 컴팩션이 되기를 기다린 후 결과 확인
    for msg in cons:
        print(msg.key, msg.value)
        if msg.key in ('Bob', 'Lucy'):
            # 테일 세그먼트에는 중복이 없음
            assert msg.value == '200'
        if msg.key == 'Patric':
            # 헤드 세그먼트의 메시지 중복
            assert msg.value in ('200', '300')


S3SK_NUM_MSG = 1000
@pytest.mark.parametrize('xs3sink', [{'flush_size': S3SK_NUM_MSG // 3}], indirect=True)
def test_s3sink(xprofile, xcp_setup, xtopic, xkfssh, xs3sink):
    """토픽에 올린 데이터가 S3 로 잘 Sink 되는지"""
    # 토픽에 가짜 데이터 생성
    procs = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, S3SK_NUM_MSG))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    # rotate.schedule.interval.ms 가 지나도록 대기
    time.sleep(10)

    tot_msg = NUM_PRO_PROCS * S3SK_NUM_MSG
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, S3 Messages: {s3cnt}")
    assert tot_msg == s3cnt


s3rr_hash = _hash()
s3rr_param = {
    # 전체 메시지 수는 NUM_MSG * NUM_PRO_PROCS 이고, 이것을 파티션별로 나눠가진다.
    # 하나 이상의 파일이 생기도록 적당히 flush
    'flush_size': S3SK_NUM_MSG // 3,
    'chash': s3rr_hash
}
@pytest.mark.parametrize('xs3sink', [s3rr_param], indirect=True)
def test_s3sink_rereg(xprofile, xcp_setup, xtopic, xkfssh, xs3sink):
    """S3 Sink 커넥터를 재등록 테스트.

    - S3 Sink 는 전체 파이프라인에서 병목이 될 가능성 높음
    - 최적화를 위해 잦은 패러미터 튜닝이 필요할 수 있음
    - 이를 위해서는 커넥터 delete 후 재등록 해야하는데.. 안전할까?
     => 커넥터 이름만 같게 해주면 일반적으로 문제 없음 (메시지 중복은 발생 가능)

    """
    # 토픽에 가짜 데이터 생성
    # Producer 프로세스 시작
    procs = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, S3SK_NUM_MSG))
        p.start()
        procs.append(p)

    time.sleep(3)
    # 잠시 후 S3 Sink 재등록 (설정 바꾸는 상황 가정)
    cname = f"s3sink-nodb-{s3rr_hash}"
    unregister_kconn(xkfssh, cname)
    time.sleep(3)
    register_s3sink(xkfssh, xprofile, 'nodb-person', s3rr_param)

    for p in procs:
        p.join()

    # rotate.schedule.interval.ms 가 지나도록 대기
    time.sleep(10)

    tot_msg = NUM_PRO_PROCS * S3SK_NUM_MSG
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, S3 Messages: {s3cnt}")
    assert tot_msg == s3cnt

decoder = lambda x: x.decode('utf-8')


S3SBK_NUM_MSG = 4000
@pytest.mark.parametrize('xs3sink', [{'flush_size': S3SBK_NUM_MSG // 3}], indirect=True)
def test_s3sink_brk(xkafka, xprofile, xcp_setup, xtopic, xkfssh, xs3sink):
    """브로커가 죽은 후 떠도 S3 Sink 가 잘 되는지.

    프로듀서가 메시지를 모두(=프로세스당 1000개) 보낸 후 브로커가 죽는 경우
    - 지연이 있으나 최종적으로는 S3 에 모든 메시지가 올라감

    프로듀서가 메시지를 모두 보내지 못한(=프로세스당 총 4000개 중 1000개) 보낸 후 브로커가 죽는 경우
    - 토픽 레벨에서 메시지 손실 발생
    - 토픽에 있는 메시지는 모두 S3 에 올라감

    """
    # 토픽에 가짜 데이터 생성
    procs = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, S3SBK_NUM_MSG))
        p.start()
        procs.append(p)

    # 잠시 후 카프카 브로커 강제 종료
    time.sleep(1)
    kill_proc_by_port(xkfssh, 9092)
    # 잠시 후 카프카 브로커 start
    time.sleep(1)
    start_kafka_broker(xkfssh)

    for p in procs:
        p.join()

    # 이 경우는 rotate.schedule.interval.ms 가 지난 후 더 기다려야함
    time.sleep(10)

    tot_msg = NUM_PRO_PROCS * S3SBK_NUM_MSG
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, S3 Messages: {s3cnt}")
    assert tot_msg == s3cnt


def test_s3():
    tot_msg = NUM_PRO_PROCS * S3SBK_NUM_MSG
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, S3 Messages: {s3cnt}")
    assert tot_msg == s3cnt