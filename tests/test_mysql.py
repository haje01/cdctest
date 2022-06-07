import os
import sys
import json

import pytest
from mysql.connector import connect

from common import SSH, setup, claim_topic, delete_topic, register_src_conn, unregister_src_conn, list_src_conns, unregister_all_src_conns

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'mysql_jdbc'))
from util import insert_fake


@pytest.fixture
def dbconn(setup):
    mysql_addr = setup['mysql_public_ip']['value']
    mysql_user = setup['db_user']['value']
    mysql_passwd = setup['db_passwd']['value']
    conn = connect(host=mysql_addr, user=mysql_user, password=mysql_passwd, db='test')
    yield conn
    conn.close()


@pytest.fixture
def dbcur(dbconn):
    cursor = dbconn.cursor()
    return cursor


@pytest.fixture
def dbinit(dbcur, setup):
    """테스트용 DB 초기화."""
    user = setup['db_user']['value']
    passwd = setup['db_passwd']['value']

    stmt = f'''
    CREATE DATABASE IF NOT EXISTS test;

    CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY '{passwd}';
    GRANT ALL PRIVILEGES ON test.* TO '{user}'@'%';
    '''
    dbcur.execute(stmt, multi=True)
    yield
    stmt = f'''
    GRANT USAGE ON *.* TO '{user}'@'%';
    DROP USER '{user}'@'%';

    DROP DATABASE IF EXISTS test;
    '''
    dbcur.execute(stmt, multi=True)


@pytest.fixture
def table(dbinit, dbcur):
    """테스트용 테이블 초기화."""
    stmt = '''
DROP TABLE IF EXISTS person;
CREATE TABLE person (
    id  INT NOT NULL AUTO_INCREMENT,
    pid INT NOT NULL,
    sid INT NOT NULL,
    name VARCHAR(40),
    address VARCHAR(200),
    ip VARCHAR(20),
    birth DATE,
    company VARCHAR(40),
    phone VARCHAR(40),
    PRIMARY KEY(id)
)
    '''
    dbcur.execute(stmt, multi=True)
    import pdb; pdb.set_trace()
    yield
    import pdb; pdb.set_trace()
    dbcur.execute('DROP TABLE IF EXISTS person;')


@pytest.fixture
def topic(setup):
    """테스트용 카프카 토픽 초기화."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    ssh = SSH(consumer_ip)

    claim_topic(ssh, kafka_ip, "my_topic_person")
    yield
    delete_topic(ssh, kafka_ip, "my_topic_person")


@pytest.fixture
def sconn(setup, table, topic):
    """테스트용 카프카 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    import pdb; pdb.set_trace()
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    mysql_addr = setup['mysql_public_ip']['value']
    mysql_user = setup['db_user']['value']
    mysql_passwd = setup['db_passwd']['value']

    ssh = SSH(consumer_ip)

    unregister_all_src_conns(ssh, kafka_ip)
    conns = register_src_conn(ssh, kafka_ip, "my-src-conn", 'mysql', mysql_addr, 3306, mysql_user, mysql_passwd, "person", "my_topic_")
    yield
    unregister_src_conn(ssh, kafka_ip, 'my-src-conn')


def test_src_conn(setup):
    """카프카 JDBC Source 커넥트 테스트."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    mysql_addr = setup['mysql_public_ip']['value']
    mysql_user = setup['db_user']['value']
    mysql_passwd = setup['db_passwd']['value']

    ssh = SSH(consumer_ip)

    # 기존 등록된 소스 커넥터 모두 제거
    unregister_all_src_conns(ssh, kafka_ip)

    # 현재 등록된 커넥터
    ret = list_src_conns(ssh, kafka_ip)
    assert ret == []

    # 커넥터 등록
    conns = register_src_conn(ssh, kafka_ip, "my-src-conn", 'mysql', mysql_addr, 3306, mysql_user, mysql_passwd, "person", "my_topic_")
    cfg = conns['config']
    assert cfg['name'] == 'my-src-conn'
    ret = list_src_conns(ssh, kafka_ip)
    assert ret == ["my-src-conn"]

    # 커넥터 해제
    unregister_src_conn(ssh, kafka_ip, 'my-src-conn')
    ret = list_src_conns(ssh, kafka_ip)
    assert ret == []


def test_ct_basic(setup, sconn):
    """기본적인 Change Tracking 테스트."""
    # DB 테이블에 insert
    insert_fake(setup, 100, 100)

    # 잠시 후 카프카 토픽 확인
    import pdb; pdb.set_trace()