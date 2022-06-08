import os
import sys
import json

import pytest
from mysql.connector import connect, DatabaseError

from common import (SSH, setup, claim_topic, delete_topic, register_sconn,
    unregister_sconn, list_sconns, unregister_all_sconns,
    count_topic_message
)

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'mysql_jdbc'))
from util import insert_fake


def exec_many(cursor, stmt):
    """멀티 라인 쿼리 실행"""
    results = cursor.execute(stmt, multi=True)
    for res in results:
        print(res)


@pytest.fixture
def dbconcur(setup):
    mysql_addr = setup['mysql_public_ip']['value']
    mysql_user = setup['db_user']['value']
    mysql_passwd = setup['db_passwd']['value']
    conn = connect(host=mysql_addr, user=mysql_user, password=mysql_passwd, database="test")
    cursor = conn.cursor()
    yield conn, cursor
    conn.close()


# @pytest.fixture
# def dbinit(dbconcur, setup):
#     """테스트용 DB 초기화."""
#     user = setup['db_user']['value']
#     passwd = setup['db_passwd']['value']
#     conn, cursor = dbconcur

#     stmt = f'''
#     CREATE DATABASE IF NOT EXISTS test;

#     CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY '{passwd}';
#     GRANT ALL PRIVILEGES ON test.* TO '{user}'@'%';
#     '''
#     exec_many(cursor, stmt)
#     yield conn, cursor

#     stmt = f'''
#     GRANT USAGE ON *.* TO '{user}'@'%';
#     DROP USER '{user}'@'%';

#     DROP DATABASE IF EXISTS test;
#     '''
#     exec_many(cursor, stmt)
#     conn.commit()


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
    cursor.execute('DROP TABLE IF EXISTS person;')
    conn.commit()


@pytest.fixture
def topic(setup):
    """테스트용 카프카 토픽 초기화."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    ssh = SSH(consumer_ip)

    claim_topic(ssh, kafka_ip, "my-topic-person")
    yield
    delete_topic(ssh, kafka_ip, "my-topic-person")


@pytest.fixture
def sconn(setup, table, topic):
    """테스트용 카프카 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    mysql_addr = setup['mysql_private_ip']['value']
    mysql_user = setup['db_user']['value']
    mysql_passwd = setup['db_passwd']['value']

    ssh = SSH(consumer_ip)

    unregister_all_sconns(ssh, kafka_ip)
    ret = register_sconn(ssh, kafka_ip, 'mysql',
        mysql_addr, 3306, mysql_user, mysql_passwd, "test", "person",
        "my-topic-")
    conn_name = ret['name']
    yield
    unregister_sconn(ssh, kafka_ip, conn_name)


def test_sconn(setup):
    """카프카 JDBC Source 커넥트 테스트."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    mysql_addr = setup['mysql_public_ip']['value']
    mysql_user = setup['db_user']['value']
    mysql_passwd = setup['db_passwd']['value']

    ssh = SSH(consumer_ip)

    # 기존 등록된 소스 커넥터 모두 제거
    unregister_all_sconns(ssh, kafka_ip)

    # 현재 등록된 커넥터
    ret = list_sconns(ssh, kafka_ip)
    assert ret == []

    # 커넥터 등록
    ret = register_sconn(ssh, kafka_ip, 'mysql', mysql_addr, 3306,
        mysql_user, mysql_passwd, "test", "person", "my-topic-")
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
    print("init")
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    ssh = SSH(consumer_ip)

    print("insert")
    # DB 테이블에 insert
    insert_fake(setup, 100, 100)

    # 카프카 토픽 확인
    print("count")
    cnt = count_topic_message(ssh, kafka_ip, 'my-topic-person')
    assert 10000 == cnt