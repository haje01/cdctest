import time
import io
import json
from pathlib import Path
import argparse

from mysql.connector import connect

# CLI 용 파서
parser = argparse.ArgumentParser(description="MySQL DB 에서 데이터 셀렉트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('setup', type=argparse.FileType('r'), help="배포 결과 파일.")
parser.add_argument('--db-name', type=str, default='test', help="MySQL 데이터베이스 이름")
parser.add_argument('-b', '--batch', type=int, default=10000, help="한 번에 select 할 행수.")
parser.add_argument('-p', '--pid', type=int, default=0, help="셀렉트 프로세스 ID.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


def count_rows(cursor):
    cursor.execute('''
    SELECT COUNT(*) cnt
    FROM person
    ''')
    res = cursor.fetchone()
    return res[0]


def select_fake(setup, db_name=parser.get_default('db_name'),
        batch=parser.get_default('batch'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('devs')
        ):
    """MySQL용 가짜 데이터 인서트.

    `db_name` DB 에 `person` 테이블이 미리 만들어져 있어야 함.

    Args:
        setup (str): 배포 결과 파일 경로
        db_name (str): DB 이름
        batch (int): 한 번에 select 할 행수
        pid (int): 멀티 프로세스 인서트시 구분용 ID
        dev (bool): 개발 PC 에서 실행 여부

    """
    if type(setup) is io.TextIOWrapper:
        setup = json.loads(setup.read())
    mysql_ip_key = 'mysql_public_ip' if dev else 'mysql_private_ip'
    db_host = setup[mysql_ip_key]['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']

    print(f"Selector {pid} connect SQL Server at {db_host}")
    conn = connect(host=db_host, user=db_user, password=db_passwd, db=db_name)
    cursor = conn.cursor()
    print("Done")

    sql = f'''
        SELECT * FROM (
            SELECT * FROM person ORDER BY pid, sid DESC LIMIT {batch}
        ) sub
        '''

    st = time.time()
    tot_read = row_cnt = i = 0
    row_prev = count_rows(cursor)
    equal = 0
    while True:
        i += 1
        print(f"row_prev: {row_prev}, row_cnt: {row_cnt}")
        time.sleep(1)
        cursor.execute(sql)
        tot_read += len(cursor.fetchall())
        row_cnt = count_rows(cursor)
        if row_cnt == row_prev:
            equal += 1
        else:
            equal = 0
        if equal > 5:
            break
        row_prev = row_cnt

    conn.close()

    elapsed = time.time() - st
    vel = tot_read / elapsed
    print(f"Select {tot_read} rows. {int(vel)} rows per seconds with batch of {batch}.")


if __name__ == '__main__':
    args = parser.parse_args()
    select_fake(args.setup, args.db_name, args.batch, args.pid, args.dev)