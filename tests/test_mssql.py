import os
import pdb
import sys
import json

import pytest
import pymssql

from common import (SSH, register_sconn, unregister_sconn, list_sconns,
    unregister_all_sconns, count_topic_message, topic, remote_insert_fake
)

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from util import mssql_insert_fake

SETUP_PATH = '../mssql/temp/setup.json'


@pytest.fixture(scope="session")
def setup():
    """인프라 설치 정보."""
    print("fixture - setup")
    assert os.path.isfile(SETUP_PATH)
    with open(SETUP_PATH, 'rt') as f:
        return json.loads(f.read())


@pytest.fixture
def dbconcur(setup):
    print("fixture - dbconcur")
    db_addr = setup['sqlserver_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']
    conn = pymssql.connect(db_addr, db_user, db_passwd, 'test')
    cursor = conn.cursor(as_dict=True)
    yield conn, cursor
    conn.close()


@pytest.fixture
def table(dbconcur):
    """테스트용 테이블 초기화."""
    print("fixture - table")
    conn, cursor = dbconcur

    stmt = '''
    IF OBJECT_ID('person', 'U') IS NOT NULL
        DROP TABLE person
    CREATE TABLE person (
        id int IDENTITY(1,1) PRIMARY KEY,
        pid INT NOT NULL,
        sid INT NOT NULL,
        name VARCHAR(40),
        address VARCHAR(200),
        ip VARCHAR(20),
        birth DATE,
        company VARCHAR(40),
        phone VARCHAR(40),
    )
    '''
    cursor.execute(stmt)
    conn.commit()
    print("  yield")
    yield
    cursor.execute('''
        IF OBJECT_ID('person', 'U') IS NOT NULL
        DROP TABLE person
    ''')
    conn.commit()


@pytest.fixture
def sconn(setup, table, topic):
    """테스트용 카프카 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    db_addr = setup['sqlserver_private_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']

    ssh = SSH(consumer_ip)

    unregister_all_sconns(ssh, kafka_ip)
    ret = register_sconn(ssh, kafka_ip, 'sqlserver',
        db_addr, 1433, db_user, db_passwd, "test", "person",
        "my-topic-")
    conn_name = ret['name']
    yield
    unregister_sconn(ssh, kafka_ip, conn_name)


def test_sconn(setup):
    """카프카 JDBC Source 커넥트 테스트."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    db_addr = setup['sqlserver_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']

    ssh = SSH(consumer_ip)

    # 기존 등록된 소스 커넥터 모두 제거
    unregister_all_sconns(ssh, kafka_ip)

    # 현재 등록된 커넥터
    ret = list_sconns(ssh, kafka_ip)
    assert ret == []

    # 커넥터 등록
    ret = register_sconn(ssh, kafka_ip, 'sqlserver', db_addr, 1433,
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


def test_ct_basic(setup, sconn):
    """기본적인 Change Tracking 테스트."""
    ins_ip = setup['inserter_public_ip']['value']
    con_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    ins_ssh = SSH(ins_ip)
    con_ssh = SSH(con_ip)

    # 인서트 노드에서 DB 테이블에 100 x 100 행 insert
    remote_insert_fake(ins_ssh, 1, 100, 100)

    # 카프카 토픽 확인
    cnt = count_topic_message(con_ssh, kafka_ip, 'my-topic-person')
    assert 10000 == cnt

    # 인서트 노드에서 DB 테이블에 100 x 100 행 insert
    remote_insert_fake(ins_ssh, 1, 100, 100)

    cnt = count_topic_message(con_ssh, kafka_ip, 'my-topic-person')
    assert 20000 == cnt
