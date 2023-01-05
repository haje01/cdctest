import os
import io
import time
import random
import argparse
import json
from multiprocessing import Process

import pymssql
from mysql.connector import connect

from kfktest.util import insert_fake, load_setup, DB_BATCH, DB_EPOCH, linfo

# CLI 용 파서
parser = argparse.ArgumentParser(description="DB 에 가짜 데이터 인서트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('db_type', type=str, choices=['mysql', 'mssql'], help="DBMS 종류.")
parser.add_argument('--db-name', type=str, default='test', help="이용할 데이터베이스 이름. 하나 이상인 경우 ',' 로 구분.")
parser.add_argument('-t', '--table', type=str, default='person', help="대상 테이블 이름.")
parser.add_argument('-p', '--pid', type=int, default=0, help="인서트 프로세스 ID.")
parser.add_argument('-e', '--epoch', type=int, default=DB_EPOCH, help="에포크 수.")
parser.add_argument('-b', '--batch', type=int, default=DB_BATCH, help="에포크당 행수.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행.")
parser.add_argument('-n', '--no-result', action='store_true', default=False,
    help="출력 감추기.")
parser.add_argument('--delay', type=int, default=0, help="여러 테이블에 인서트시 지연 시간 범위 (초).")
parser.add_argument('--dt', type=str, default=None, help="지정된 일시로 테이블에 인서트.")
parser.add_argument('--db-host', type=str, help="외부 MySQL DB 주소.")
parser.add_argument('--db-user', type=str, help="외부 MySQL DB 유저.")
parser.add_argument('--db-passwd', type=str, help="외부 MySQL DB 암호.")


def insert(db_type,
        db_name=parser.get_default('db_name'),
        table=parser.get_default('table'),
        epoch=parser.get_default('epoch'),
        batch=parser.get_default('batch'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('devs'),
        no_result=parser.get_default('no_result'),
        delay=0,
        dt=None,
        show=False,
        db_host=None,
        db_user=None,
        db_passwd=None
        ):
    """가짜 데이터 인서트.

    `db_name` DB 에 `person` 테이블이 미리 만들어져 있어야 함.

    Args:
        db_type (str): DBMS 종류. mysql / mssql
        db_name (str): DB 이름
        table (str): 테이블 이름. 기본값 person
        epoch (int): 에포크 수
        batch (int): 에포크당 배치 수
        pid (int): 멀티 프로세스 인서트시 구분용 ID
        dev (bool): 개발 PC 에서 실행 여부
        no_result (bool): 결과 감추기 여부. 기본값 True
        delay (int): 지연 시간. 기본값 0
        dt (str): 지정된 일시. 기본값 None
        show (bool): fake 메시지 표시 여부. 기본값 False

    """
    # 프로세스간 commit 이 몰리지 않게
    time.sleep(random.random() * delay)

    # 외부 DB 정보가 없으면 생성한 DB
    if db_host is None:
        setup = load_setup(db_type)
        if db_type == 'mysql':
            db_ip_key = 'mysql_public_ip' if dev else 'mysql_private_ip'
        else:
            db_ip_key = 'mssql_public_ip' if dev else 'mssql_private_ip'
        db_host = setup[db_ip_key]['value']
        db_user = setup['db_user']['value']
        db_passwd = setup['db_passwd']['value']['result']

    linfo(f"Inserter {pid} connect DB at {db_host}")
    if db_type == 'mysql':
        conn = connect(host=db_host, user=db_user, password=db_passwd, db=db_name)
    else:
        conn = pymssql.connect(host=db_host, user=db_user, password=db_passwd, database=db_name)
    cursor = conn.cursor()
    linfo("Connect done.")

    st = time.time()
    insert_fake(conn, cursor, epoch, batch, pid, db_type, table=table, dt=dt, show=show)
    conn.close()

    elapsed = time.time() - st
    vel = epoch * batch / elapsed
    if not no_result:
        linfo(f"Inserter {pid} inserted {batch * epoch} rows. {int(vel)} rows per seconds with batch of {batch}.")


if __name__ == '__main__':
    args = parser.parse_args()
    tables = [tbl.strip() for tbl in args.table.split(',')]
    if len(tables) == 1:
        insert(args.db_type, args.db_name, args.table, args.epoch, args.batch,
            args.pid, args.dev, args.no_result)
    else:
        # 테이블이 하나 이상 지정되면 병렬 처리
        procs = []
        for table in tables:
            p = Process(target=insert, args=(args.db_type, args.db_name,
                                             table, args.epoch, args.batch,
                                             args.pid, args.dev, args.no_result,
                                             args.delay, args.dt, False,
                                             args.db_host, args.db_user, args.db_passwd
                                             ))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()


