import os
import sys
import time
import argparse

import pymssql
from sqlalchemy import insert

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from util import _insert_fake


def insert_fake(db_host, db_user, db_passwd, db_name, epoch, batch, pid):
    """MSSQL용 가짜 데이터 인서트.

    Args:
        db_host (str): DB 서버 주소
        db_user (str): DB 유저
        db_passwd (str): DB 유저 암호
        db_name (str): DB 이름
        epoch (int): 에포크 수
        batch (int): 에포크당 배치 수
        pid (str): 멀티 프로세스 인서트시 구분용 ID

    """
    print(f"Inserter {pid} connect SQL Server at {db_host}")
    conn = pymssql.connect(db_host, db_user, db_passwd, db_name)
    cursor = conn.cursor(as_dict=True)
    print("Done")

    st = time.time()
    _insert_fake(conn, cursor, epoch, batch, pid)
    conn.close()

    elapsed = time.time() - st
    vel = epoch * batch / elapsed
    print(f"Insert {batch * epoch} rows. {int(vel)} rows per seconds with batch of {batch}.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MSSQL DB 에 가짜 데이터 인서트.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('db_host', type=str, help="MSSQL 서버 주소")
    parser.add_argument('db_user', type=str, help="MSSQL 유저")
    parser.add_argument('db_passwd', type=str, help="MSSQL 유저 암호")
    parser.add_argument('-d', '--db-name', type=str, default='test', help="MSSQL 데이터베이스 이름")
    parser.add_argument('-p', '--pid', type=int, default=0, help="인서트 프로세스 ID.")
    parser.add_argument('-e', '--epoch', type=int, default=100, help="에포크 수.")
    parser.add_argument('-b', '--batch', type=int, default=100, help="에포크당 행수.")

    args = parser.parse_args()

    insert_fake(args.db_host, args.db_user, args.db_passwd, args.db_name,
        args.epoch, args.batch, args.pid)