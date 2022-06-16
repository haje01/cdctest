import multiprocessing


from multiprocessing import Process

import pytest

from kfktest.util import setup, local_exec


@pytest.fixture(scope="session")
def profile():
    return 'minimal'


def _local_produce_proc(setup):
    """로컬 프로듀서."""
    print("Produce process start.")
    cmd = f'python -m kfktest.producer -d'
    local_exec(cmd)
    print("Produce process done.")


def test_local_basic(setup):
    """로컬 프로듀서 및 컨슈머로 기본 동작 테스트."""
    # Producer 프로세스 시작
    pro = Process(target=_local_produce_proc, args=(setup,))

    # Consumer 프로세스 시작
