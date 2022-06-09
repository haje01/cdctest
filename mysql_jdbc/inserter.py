import os
import io
import time
import sys
import argparse
import json

from mysql.connector import connect

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from util import _insert_fake

# CLI 용 파서
parser = argparse.ArgumentParser(description="MySQL DB 에 가짜 데이터 인서트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('setup', type=argparse.FileType('r'), help="배포 결과 파일.")
parser.add_argument('--db-name', type=str, default='test', help="MySQL 데이터베이스 이름")
parser.add_argument('-p', '--pid', type=int, default=0, help="인서트 프로세스 ID.")
parser.add_argument('-e', '--epoch', type=int, default=100, help="에포크 수.")
parser.add_argument('-b', '--batch', type=int, default=100, help="에포크당 행수.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


def insert_fake(setup, db_name=parser.get_default('db_name'),
        epoch=parser.get_default('epoch'),
        batch=parser.get_default('batch'),
        pid=parser.get_default('pid'),
        dev=parser.get_default('devs')
        ):
    """MySQL용 가짜 데이터 인서트.

    `db_name` DB 에 `person` 테이블이 미리 만들어져 있어야 함.

    Args:
        setup (str): 배포 결과 파일 경로
        db_name (str): DB 이름
        epoch (int): 에포크 수
        batch (int): 에포크당 배치 수
        pid (int): 멀티 프로세스 인서트시 구분용 ID
        dev (bool): 개발 PC 에서 실행 여부

    """
    if type(setup) is io.TextIOWrapper:
        setup = json.loads(setup.read())
    mysql_ip_key = 'mysql_public_ip' if dev else 'mysql_private_ip'
    db_host = setup[mysql_ip_key]['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']

    print(f"Inserter {pid} connect MySQL at {db_host}")
    conn = connect(host=db_host, user=db_user, password=db_passwd, db=db_name)
    cursor = conn.cursor()
    print("Done")

    st = time.time()
    _insert_fake(conn, cursor, epoch, batch, pid)
    conn.close()

    elapsed = time.time() - st
    vel = epoch * batch / elapsed
    print(f"Insert {batch * epoch} rows. {int(vel)} rows per seconds with batch of {batch}.")


if __name__ == '__main__':
    args = parser.parse_args()
    insert_fake(args.setup, args.db_name, args.epoch, args.batch, args.pid, args.dev)