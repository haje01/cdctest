import time
from multiprocessing import Process, Queue

import pytest

from kfktest.util import (local_produce_proc, local_consume_proc, local_exec,
    linfo, remote_produce_proc, count_topic_message, s3_count_sinkmsg,
    KFKTEST_S3_BUCKET, KFKTEST_S3_DIR, unregister_kconn, register_s3sink,
    _hash,
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


def test_local_basic(xprofile, xsetup, xtopic, xkfssh):
    """로컬 프로듀서 및 컨슈머로 기본 동작 테스트."""
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, PROC_NUM_MSG))
        p.start()
        pro_pros.append(p)

    # Consumer 프로세스 시작
    q = Queue()
    con = Process(target=local_consume_proc, args=(xprofile, 1, q))
    con.start()

    for p in pro_pros:
        p.join()
    con_cnt = q.get()
    con.join()

    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. {int(vel)} rows per seconds.")
    assert tot_msg == con_cnt


def test_remote_basic(xprofile, xsetup, xcp_setup, xtopic, xkfssh):
    """원격 프로듀서 및 컨슈머로 기본 동작 테스트."""
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=remote_produce_proc, args=(xprofile, xsetup, pid, PROC_NUM_MSG))
        p.start()
        pro_pros.append(p)

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xprofile, xtopic, timeout=10)
    for p in pro_pros:
        p.join()

    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. {int(vel)} rows per seconds.")
    assert tot_msg == cnt


S3SK_NUM_MSG = 1000
@pytest.mark.parametrize('xs3sink', [{'flush_size': S3SK_NUM_MSG // 3}], indirect=True)
def test_s3sink(xprofile, xcp_setup, xtopic, xkfssh, xs3sink):
    # 토픽에 가짜 데이터 생성
    # Producer 프로세스 시작
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


def test_s3():
    tot_msg = NUM_PRO_PROCS * S3SK_NUM_MSG
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, S3 Messages: {s3cnt}")
    assert tot_msg == s3cnt
