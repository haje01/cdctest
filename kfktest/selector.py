import time
import io
import json
from pathlib import Path
import argparse

import pymssql
from mysql.connector import connect

from kfktest.util import load_setup, count_rows, linfo

# CLI 용 파서
parser = argparse.ArgumentParser(description="MySQL DB 에서 데이터 셀렉트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('db_type', type=str, choices=['mysql', 'mssql'], help="DBMS 종류.")
parser.add_argument('--db-name', type=str, default='test', help="이용할 데이터베이스 이름")
parser.add_argument('-b', '--batch', type=int, default=1000, help="한 번에 select 할 행수.")
parser.add_argument('-p', '--pid', type=int, default=0, help="셀렉트 프로세스 ID.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행.")


def select(db_type, db_name=parser.get_default('db_name'),
        batch=parser.get_default('batch'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('devs')
        ):
    """DB 에서 가짜 데이터 셀렉트.

    실제 라이브와 비슷한 상황에서 CDC/CT 하기 위함.

    `db_name` DB 에 `person` 테이블이 미리 만들어져 있어야 함.

    Args:
        db_type (str): DBMS 종류. mysql / mssql
        db_name (str): DB 이름
        batch (int): 한 번에 select 할 행수
        pid (int): 멀티 프로세스 인서트시 구분용 ID
        dev (bool): 개발 PC 에서 실행 여부

    Returns:
        int: 읽은 행 수 (테이블 행 수와 일치하지 않음!)

    """
    setup = load_setup(db_type)
    db_ip_key = f'{db_type}_public_ip' if dev else f'{db_type}_private_ip'
    db_host = setup[db_ip_key]['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']['result']

    linfo(f"Selector {pid} connect DB at {db_host} batch {batch}")
    if db_type == 'mysql':
        conn = connect(host=db_host, user=db_user, password=db_passwd, db=db_name)
    else:
        conn = pymssql.connect(host=db_host, user=db_user, password=db_passwd, database=db_name)
    cursor = conn.cursor()
    linfo("Connect done.")

    if db_type == 'mysql':
        sql = f'''
            SELECT * FROM (
                SELECT * FROM person ORDER BY pid, sid DESC LIMIT {batch}
            ) sub
            '''
    else:
        # MSSQL
        cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
        sql = f'''
            SELECT TOP {batch} *
            FROM [test].[dbo].[person]
            ORDER BY newid()
            '''

    st = time.time()
    tot_read = row_cnt = i = 0
    row_prev = count_rows(db_type, cursor)
    equal = 0
    while True:
        i += 1
        linfo(f"Selector {pid} row_prev: {row_prev}, row_cnt: {row_cnt} equal {equal}")
        conn.commit()
        time.sleep(1)
        cursor.execute(sql)
        tot_read += len(cursor.fetchall())
        row_cnt = count_rows(db_type, cursor)
        if row_cnt == row_prev:
            equal += 1
        else:
            equal = 0
        if equal >= 5:
            break
        row_prev = row_cnt

    conn.close()

    elapsed = time.time() - st
    vel = tot_read / elapsed
    linfo(f"Selector {pid} selects {tot_read} rows. {int(vel)} rows per seconds.")
    return tot_read


if __name__ == '__main__':
    args = parser.parse_args()
    select(args.db_type, args.db_name, args.batch, args.pid,
        args.dev)