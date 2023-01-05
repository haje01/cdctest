import argparse

from kfktest.util import mysql_exec_many, db_concur, linfo, _db_concur


# CLI 용 파서
parser = argparse.ArgumentParser(description="MySQL DB 에 가짜 데이터 인서트.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('db_type', type=str, choices=['mysql', 'mssql'], help="DBMS 종류.")
parser.add_argument('--db-name', type=str, default='test', help="이용할 데이터베이스 이름.")
parser.add_argument('-t', '--table', type=str, default='person', help="이용할 테이블 이름.")
parser.add_argument('--db-host', type=str, help="외부 MySQL DB 주소.")
parser.add_argument('--db-user', type=str, help="외부 MySQL DB 유저.")
parser.add_argument('--db-passwd', type=str, help="외부 MySQL DB 암호.")


def reset_table(profile, table, fix_regdt=None, concur=None, datetime1=False,
        db_host=None, db_user=None, db_passwd=None):
    regdt_def = 'CURRENT_TIMESTAMP' if fix_regdt is None else f"'{fix_regdt}'"
    linfo(f"[ ] reset_table for {profile} {table}")
    if concur is None:
        if db_host is None:
            conn, cursor = db_concur(profile)
        else:
            conn, cursor = _db_concur(profile, db_host, db_user, db_passwd)
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
        # DATETIME2 가 더 정밀해 중복 위험 때문에 권고 사항이나 테스트를 위해 DATETIME 이용가
        # DATETIME 컬럼에 대해 Timestamp 모드 사용시 로그 유실이 드물지 않게 발생
        #  -> timestamp.delay.interval.ms 로 대응
        dt_type = 'DATETIME' if datetime1 else 'DATETIME2'
        head = f'''
    IF OBJECT_ID('{table}', 'U') IS NOT NULL
        DROP TABLE {table}
    CREATE TABLE {table} (
        id int IDENTITY(1,1) PRIMARY KEY,
        regdt {dt_type} DEFAULT {regdt_def} NOT NULL,
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
    reset_table(args.db_type, args.table, None, None, False, 
        args.db_host, args.db_user, args.db_passwd)