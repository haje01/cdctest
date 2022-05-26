import time
import sys
import json
from pathlib import Path

from mysql.connector import connect


with open(snakemake.input[0], 'rt') as f:
    tfout = json.loads(f.read())

SERVER = tfout['mysql_public_ip']['value']
USER = tfout['db_user']['value']
PASSWD = tfout['db_passwd']['value']
DATABASE = 'test'

print(f"Connect SQL Server at {SERVER}")
conn = connect(host=SERVER, user=USER, password=PASSWD, database=DATABASE)
cursor = conn.cursor()
print("Done")

# 테이블 생성
sql = '''
DROP TABLE IF EXISTS person;
CREATE TABLE person (
    pid INT NOT NULL,
    sid INT NOT NULL,
    name VARCHAR(40),
    address VARCHAR(200),
    ip VARCHAR(20),
    birth DATE,
    company VARCHAR(40),
    phone VARCHAR(40),
    PRIMARY KEY(pid, sid)
)
'''
cursor.execute(sql)
conn.close()

Path(snakemake.output[0]).touch()
