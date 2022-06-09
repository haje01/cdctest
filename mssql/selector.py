import time
import io
import json
from pathlib import Path
import argparse

import pymssql

# CLI 용 파서
parser = argparse.ArgumentParser(description="MSSQL DB 에서 데이터 셀렉트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('setup', type=argparse.FileType('r'), help="배포 결과 파일.")
parser.add_argument('--db-name', type=str, default='test', help="MSSQL 데이터베이스 이름")
parser.add_argument('-b', '--batch', type=int, default=1000, help="한 번에 select 할 행수.")
parser.add_argument('-p', '--pid', type=int, default=0, help="셀렉트 프로세스 ID.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


def count_rows(cursor):
    cursor.execute('''
    SELECT COUNT(*) cnt
    FROM [test].[dbo].[person]
    ''')
    res = cursor.fetchone()
    return res[0]


def select_fake(setup, db_name=parser.get_default('db_name'),
        batch=parser.get_default('batch'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('devs')
        ):
    """MSSQL 용 가짜 데이터 인서트.

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
    mssql_ip_key = 'mssql_public_ip' if dev else 'mssql_private_ip'
    db_host = setup[mssql_ip_key]['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']

    print(f"Selector {pid} connect MSSQL at {db_host} batch {batch}")
    conn = pymssql.connect(host=db_host, user=db_user, password=db_passwd, database=db_name)
    cursor = conn.cursor()
    print("Connect done.")

    cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')

    sql = f'''
        SELECT TOP {batch} *
        FROM [test].[dbo].[person]
        ORDER BY newid()
        '''

    st = time.time()
    tot_read = row_cnt = i = 0
    row_prev = count_rows(cursor)
    equal = 0
    while True:
        i += 1
        print(f"Selector {pid} row_prev: {row_prev}, row_cnt: {row_cnt} equal {equal}")
        # conn.commit()
        time.sleep(1)
        cursor.execute(sql)
        tot_read += len(cursor.fetchall())
        row_cnt = count_rows(cursor)
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
    print(f"Selector {pid} selects {tot_read} rows. {int(vel)} rows per seconds.")


if __name__ == '__main__':
    args = parser.parse_args()
    select_fake(args.setup, args.db_name, args.batch, args.pid, args.dev)

# import time
# import sys
# import json
# from pathlib import Path

# import pymssql

# BATCH = 10000

# num_arg = len(sys.argv)
# assert num_arg in (2, 3)
# dev = num_arg == 2
# setup = sys.argv[1]
# pid = int(sys.argv[2]) if not dev else -1

# with open(setup, 'rt') as f:
#     setup = json.loads(f.read())

# print(f"Dev: {dev}")
# print(f"Batch: {BATCH}")
# ip = setup['mssql_public_ip'] if dev else setup['mssql_private_ip']
# SERVER = ip['value']
# # SERVER = setup['mssql_public_ip']['value']
# USER = setup['db_user']['value']
# PASSWD = setup['db_passwd']['value']
# DATABASE = 'test'

# print(f"Selector {pid} connect SQL Server at {SERVER}")
# conn = pymssql.connect(SERVER, USER, PASSWD, DATABASE)
# cursor = conn.cursor(as_dict=True)
# print("Done")

# def count_rows():
#     cursor.execute('''
#     SELECT COUNT(*) cnt
#     FROM [test].[dbo].[person]
#     ''')
#     res = cursor.fetchone()
#     return res['cnt']

# sql = f'''
#     SELECT TOP {BATCH} *
#     FROM [test].[dbo].[person]
#     ORDER BY newid()
#     '''

# st = time.time()
# tot_read = row_cnt = i = 0
# row_prev = count_rows()
# equal = 0
# while True:
#     i += 1
#     print(f"row_prev: {row_prev}, row_cnt: {row_cnt}")
#     time.sleep(1)
#     cursor.execute(sql)
#     tot_read += len(cursor.fetchall())
#     row_cnt = count_rows()
#     if row_cnt == row_prev:
#         equal += 1
#     else:
#         equal = 0
#     if equal > 5:
#         break
#     row_prev = row_cnt

# conn.close()

# elapsed = time.time() - st
# vel = tot_read / elapsed
# print(f"Select {tot_read} rows. {int(vel)} rows per seconds with batch of {BATCH}.")
