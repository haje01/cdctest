import argparse

from kfktest.util import mysql_exec_many, db_concur, linfo


# CLI 용 파서
parser = argparse.ArgumentParser(description="MySQL DB 에 가짜 데이터 인서트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('db_type', type=str, choices=['mysql', 'mssql'], help="DBMS 종류.")
parser.add_argument('--db-name', type=str, default='test', help="이용할 데이터베이스 이름.")
parser.add_argument('-t', '--table', type=str, default='person', help="이용할 테이블 이름.")


def reset_table(profile, table, fix_regdt=None, concur=None):
    regdt_def = 'CURRENT_TIMESTAMP' if fix_regdt is None else f"'{fix_regdt}'"
    linfo(f"[ ] reset_table for {profile} {table}")
    if concur is None:
        conn, cursor = db_concur(profile)
    else:
        conn, cursor = concur

    if profile == 'mysql':
        head = f'''
    DROP TABLE IF EXISTS {table};
    CREATE TABLE {table} (
        id  INT NOT NULL AUTO_INCREMENT,
        regdt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
        pid INT DEFAULT -1 NOT NULL,
        sid INT DEFAULT -1 NOT NULL,
        '''
        tail = ', PRIMARY KEY(id)'
    else:
        ## MSSQL
        # DATETIME2 가 더 정밀해 중복 위험 때문에 권고 사항이긴 하나
        # incremental+timestamp 형식으로 복합키로 사용할 때는 중복 위험 없을 듯
        head = f'''
    IF OBJECT_ID('{table}', 'U') IS NOT NULL
        DROP TABLE {table}
    CREATE TABLE {table} (
        id int IDENTITY(1,1) PRIMARY KEY,
        regdt DATETIME2 DEFAULT {regdt_def} NOT NULL,
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
    linfo(f"Create table '{table}'")
    if profile == 'mysql':
        mysql_exec_many(cursor, sql)
    else:
        cursor.execute(sql)
    conn.commit()
    linfo(f"[v] reset_table for {profile} {table}")
    return conn, cursor


if __name__ == '__main__':
    args = parser.parse_args()
    reset_table(args.db_type, args.table)