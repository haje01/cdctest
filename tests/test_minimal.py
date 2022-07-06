import time
from multiprocessing import Process, Queue

import pytest

from kfktest.util import (local_produce_proc, local_consume_proc, local_exec,
    linfo, remote_produce_proc, count_topic_message,
    # 픽스쳐들
    xsetup, xtopic, xkfssh, xkvmstart, xcp_setup
)
from kfktest.producer import produce
from kfktest.consumer import consume

NUM_PRO_PROCS = 4
PROC_NUM_MSG = 10000


@pytest.fixture(scope="session")
def xprofile():
    return 'minimal'


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
    cnt = count_topic_message(xkfssh, xtopic, timeout=10)
    for p in pro_pros:
        p.join()

    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. {int(vel)} rows per seconds.")
    assert tot_msg == cnt
