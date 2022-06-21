import argparse

from kfktest.util import mysql_exec_many, db_concur


# CLI 용 파서
parser = argparse.ArgumentParser(description="MySQL DB 에 가짜 데이터 인서트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('db_type', type=str, choices=['mysql', 'mssql'], help="DBMS 종류.")
parser.add_argument('--db-name', type=str, default='test', help="이용할 데이터베이스 이름.")
parser.add_argument('-p', '--pid', type=int, default=0, help="인서트 프로세스 ID.")
parser.add_argument('-e', '--epoch', type=int, default=100, help="에포크 수.")
parser.add_argument('-b', '--batch', type=int, default=100, help="에포크당 행수.")
parser.add_argument('-d', '--dev', action='store_true', default=False,
    help="개발 PC 에서 실행 여부.")


def reset_table(profile):
    print(f"[ ] reset_table for {profile}")
    conn, cursor = db_concur(profile)

    if profile == 'mysql':
        head = '''
    DROP TABLE IF EXISTS person;
    CREATE TABLE person (
        id  INT NOT NULL AUTO_INCREMENT,
        pid INT DEFAULT -1 NOT NULL,
        sid INT DEFAULT -1 NOT NULL,
        '''
        tail = ', PRIMARY KEY(id)'
    else:
        # MSSQL
        head = '''
    IF OBJECT_ID('person', 'U') IS NOT NULL
        DROP TABLE person
    CREATE TABLE person (
        id int IDENTITY(1,1) PRIMARY KEY,
        pid INT NOT NULL,
        sid INT NOT NULL,
            '''
        tail = ''

    sql = f'''
        {head}name VARCHAR(40),
        address VARCHAR(200),
        ip VARCHAR(20),
        birth DATE,
        company VARCHAR(40),
        phone VARCHAR(40)
        {tail}
        )
        '''
    print("Create table 'person'")
    if profile == 'mysql':
        mysql_exec_many(cursor, sql)
    else:
        cursor.execute(sql)
    conn.commit()
    print(f"[X] reset_table for {profile}")
    return conn, cursor


if __name__ == '__main__':
    args = parser.parse_args()
    reset_table(args.db_type)