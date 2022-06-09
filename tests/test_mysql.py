import os
import sys
import json
from multiprocessing import Process
from threading import local

import pytest
from mysql.connector import connect, DatabaseError

from common import (SSH, register_sconn, unregister_sconn, list_sconns,
    unregister_all_sconns, count_topic_message, topic, ssh_cmd, local_cmd,
    scp_to_remote
)

SETUP_PATH = '../mysql_jdbc/temp/setup.json'
NUM_INSEL_PROCS = 5

@pytest.fixture(scope="session")
def setup():
    """인프라 설치 정보."""
    assert os.path.isfile(SETUP_PATH)
    with open(SETUP_PATH, 'rt') as f:
        return json.loads(f.read())


@pytest.fixture(scope="session")
def cp_setup(setup):
    """확보된 인프라 설치 정보를 원격 노드에 복사."""
    targets = ['consumer_public_ip', 'inserter_public_ip', 'selector_public_ip']
    for target in targets:
        ip = setup[target]['value']
        pkey = setup['private_key_path']['value']
        scp_to_remote('../mysql_jdbc/temp/setup.json', ip, '~/cdctest/mysql_jdbc/temp', pkey)
    yield setup


def exec_many(cursor, stmt):
    """멀티 라인 쿼리 실행

    주: 결과를 읽어와야 쿼리 실행이 됨

    """
    results = cursor.execute(stmt, multi=True)
    for res in results:
        print(res)


@pytest.fixture
def dbconcur(setup):
    db_addr = setup['mysql_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']
    conn = connect(host=db_addr, user=db_user, password=db_passwd, database="test")
    cursor = conn.cursor()
    yield conn, cursor
    conn.close()


@pytest.fixture
def table(dbconcur):
    """테스트용 테이블 초기화."""
    conn, cursor = dbconcur

    stmt = '''
DROP TABLE IF EXISTS person;
CREATE TABLE person (
    id  INT NOT NULL AUTO_INCREMENT,
    pid INT DEFAULT -1 NOT NULL,
    sid INT DEFAULT -1 NOT NULL,
    name VARCHAR(40),
    address VARCHAR(200),
    ip VARCHAR(20),
    birth DATE,
    company VARCHAR(40),
    phone VARCHAR(40),
    PRIMARY KEY(id)
)
    '''
    exec_many(cursor, stmt)
    yield
    print("Delete table `person`")
    cursor.execute('DROP TABLE IF EXISTS person;')
    conn.commit()


@pytest.fixture
def sconn(setup, table, topic):
    """테스트용 카프카 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    cons_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    db_addr = setup['mysql_private_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']

    ssh = SSH(cons_ip)

    unregister_all_sconns(ssh, kafka_ip)
    ret = register_sconn(ssh, kafka_ip, 'mysql',
        db_addr, 3306, db_user, db_passwd, "test", "person",
        "my-topic-")
    conn_name = ret['name']
    yield
    unregister_sconn(ssh, kafka_ip, conn_name)


def test_sconn(setup):
    """카프카 JDBC Source 커넥트 테스트."""
    cons_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    db_addr = setup['mysql_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']

    ssh = SSH(cons_ip)

    # 기존 등록된 소스 커넥터 모두 제거
    unregister_all_sconns(ssh, kafka_ip)

    # 현재 등록된 커넥터
    ret = list_sconns(ssh, kafka_ip)
    assert ret == []

    # 커넥터 등록
    ret = register_sconn(ssh, kafka_ip, 'mysql', db_addr, 3306,
        db_user, db_passwd, "test", "person", "my-topic-")
    conn_name = ret['name']
    cfg = ret['config']
    assert cfg['name'].startswith('my-sconn')
    ret = list_sconns(ssh, kafka_ip)
    assert ret == [conn_name]

    # 커넥터 해제
    unregister_sconn(ssh, kafka_ip, conn_name)
    ret = list_sconns(ssh, kafka_ip)
    assert ret == []

def _local_select_proc(setup, pid):
    """로컬에서 가짜 데이터 셀렉트."""
    print(f"Select process {pid} start")
    cmd = f"cd ../mysql_jdbc && python selector.py temp/setup.json -p {pid} -d"
    local_cmd(cmd)
    print(f"Select process {pid} done")


def _local_insert_proc(setup, pid, epoch, batch):
    """로컬에서 가짜 데이터 인서트."""
    print(f"Insert process start: {pid}")
    cmd = f"cd ../mysql_jdbc && python inserter.py temp/setup.json -p {pid} -e {epoch} -b {batch} -d"
    local_cmd(cmd)
    print(f"Insert process done: {pid}")


def test_ct_local_basic(setup, table, sconn):
    """로컬 insert / select 로 기본적인 Change Tracking 테스트."""
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
    cnt = count_topic_message(ssh, kafka_ip, 'my-topic-person')
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
    cmd = f"cd cdctest/mysql_jdbc && python3 selector.py temp/setup.json -p {pid}"
    ret = ssh_cmd(ssh, cmd, False)
    print(ret)
    print(f"Select process done: {pid}")
    return ret


def _remote_insert_proc(setup, pid, epoch, batch):
    """원격 인서트 노드에서 가짜 데이터 인서트 (원격 노드에 temp/setup.json 있어야 함)."""
    print(f"Insert process start: {pid}")
    ins_ip = setup['inserter_public_ip']['value']
    ssh = SSH(ins_ip)
    cmd = f"cd cdctest/mysql_jdbc && python3 inserter.py temp/setup.json -p {pid} -e {epoch} -b {batch}"
    ret = ssh_cmd(ssh, cmd, False)
    print(ret)
    print(f"Insert process done: {pid}")
    return ret


def test_ct_remote_basic(cp_setup, table, sconn):
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
    cnt = count_topic_message(ssh, kafka_ip, 'my-topic-person')
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()

    for p in sel_pros:
        p.join()
