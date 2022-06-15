import os
import json
from multiprocessing import Process

import pytest
from mysql.connector import connect

from kfktest.util import (SSH, register_socon, unregister_socon, list_socons,
    unregister_all_socons, count_topic_message, topic, ssh_cmd, local_cmd,
    scp_to_remote
)

NUM_INSEL_PROCS = 5


@pytest.fixture(scope="session")
def db_type():
    return 'mysql'


def test_socon(setup):
    """카프카 JDBC Source 커넥트 테스트."""
    cons_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    db_addr = setup['mysql_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']['result']

    ssh = SSH(cons_ip)

    # 기존 등록된 소스 커넥터 모두 제거
    unregister_all_socons(ssh, kafka_ip)

    # 현재 등록된 커넥터
    ret = list_socons(ssh, kafka_ip)
    assert ret == []

    # 커넥터 등록
    ret = register_socon(ssh, kafka_ip, 'mysql', db_addr, DB_PORT,
        db_user, db_passwd, "test", "person", "my-topic-")
    conn_name = ret['name']
    cfg = ret['config']
    assert cfg['name'].startswith('my-socon')
    ret = list_socons(ssh, kafka_ip)
    assert ret == [conn_name]

    # 커넥터 해제
    unregister_socon(ssh, kafka_ip, conn_name)
    ret = list_socons(ssh, kafka_ip)
    assert ret == []


def _local_select_proc(setup, pid):
    """로컬에서 가짜 데이터 셀렉트."""
    print(f"Select process {pid} start")
    cmd = f"cd ../mysql && python -m kfktest.selector temp/setup.json mysql -p {pid} -d"
    local_cmd(cmd)
    print(f"Select process {pid} done")


def _local_insert_proc(setup, pid, epoch, batch):
    """로컬에서 가짜 데이터 인서트."""
    print(f"Insert process start: {pid}")
    cmd = f"cd ../mysql && python -m kfktest.inserter temp/setup.json mysql -p {pid} -e {epoch} -b {batch} -d"
    local_cmd(cmd)
    print(f"Insert process done: {pid}")


def test_ct_local_basic(setup, table, socon):
    """로컬 insert / select 로 기본적인 Change Tracking 테스트.

    - 테스트 시작전 이전 토픽을 참고하는 것이 없어야 함. (delete_topic 에러 발생)

    """
    cons_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    ssh = SSH(cons_ip)

    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_select_proc, args=(setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_insert_proc, args=(setup, pid, 100, 100))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(ssh, kafka_ip, 'my-topic-person', timeout=25)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")


def _remote_select_proc(setup, pid):
    """원격 셀렉트 노드에서 가짜 데이터 셀렉트 (원격 노드에 temp/setup.json 있어야 함)."""
    print(f"Select process start: {pid}")
    sel_ip = setup['selector_public_ip']['value']
    ssh = SSH(sel_ip)
    cmd = f"cd kfktest/mysql && python3 -m kfktest.selector temp/setup.json mysql -p {pid}"
    ret = ssh_cmd(ssh, cmd, False)
    print(ret)
    print(f"Select process done: {pid}")
    return ret


def _remote_insert_proc(setup, pid, epoch, batch):
    """원격 인서트 노드에서 가짜 데이터 인서트 (원격 노드에 temp/setup.json 있어야 함)."""
    print(f"Insert process start: {pid}")
    ins_ip = setup['inserter_public_ip']['value']
    ssh = SSH(ins_ip)
    cmd = f"cd kfktest/mysql && python3 -m kfktest.inserter temp/setup.json mysql -p {pid} -e {epoch} -b {batch}"
    ret = ssh_cmd(ssh, cmd, False)
    print(ret)
    print(f"Insert process done: {pid}")
    return ret


def test_ct_remote_basic(cp_setup, table, socon):
    """원격 insert / select 로 기본적인 Change Tracking 테스트."""
    cons_ip = cp_setup['consumer_public_ip']['value']
    kafka_ip = cp_setup['kafka_private_ip']['value']
    ssh = SSH(cons_ip)

    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_remote_select_proc, args=(cp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_remote_insert_proc, args=(cp_setup, pid, 100, 100))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(ssh, kafka_ip, 'my-topic-person', timeout=25)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")
