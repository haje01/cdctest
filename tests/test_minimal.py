import multiprocessing


from multiprocessing import Process, Queue

import pytest

from kfktest.util import setup, topic, local_exec
from kfktest.producer import produce
from kfktest.consumer import consume


@pytest.fixture(scope="session")
def profile():
    return 'minimal'


def _local_produce_proc(pro_cnt):
    """로컬 프로듀서."""
    print("Produce process start.")
    produce('minimal', count=pro_cnt, dev=True)
    print("Produce process done.")


def _local_consume_proc(q):
    print("Consume process start.")
    cnt = consume('minimal', dev=True)
    q.put(cnt)
    print("Consume process done.")


def test_local_basic(setup, topic):
    """로컬 프로듀서 및 컨슈머로 기본 동작 테스트."""
    pro_cnt = 10000
    # Producer 프로세스 시작
    pro = Process(target=_local_produce_proc, args=(pro_cnt,))
    pro.start()

    # Consumer 프로세스 시작
    q = Queue()
    con = Process(target=_local_consume_proc, args=(q,))
    con.start()

    pro.join()
    con_cnt = q.get()
    con.join()
    assert pro_cnt == con_cnt
