import time
import sys
import json
from pathlib import Path

from mysql.connector import connect

BATCH = 10000

num_arg = len(sys.argv)
assert num_arg in (2, 3)
dev = num_arg == 2
setup = sys.argv[1]
pid = int(sys.argv[2]) if not dev else -1

with open(setup, 'rt') as f:
    setup = json.loads(f.read())

print(f"Dev: {dev}")
print(f"Batch: {BATCH}")
ip = setup['sqlserver_public_ip'] if dev else setup['sqlserver_private_ip']
SERVER = ip['value']
# SERVER = setup['sqlserver_public_ip']['value']
USER = setup['db_user']['value']
PASSWD = setup['db_passwd']['value']
DATABASE = 'test'

print(f"{pid} Connect SQL Server at {SERVER}")
conn = connect(host=SERVER, user=USER, password=PASSWD, database=DATABASE)
cursor = conn.cursor(as_dict=True)
print("Done")

def count_rows():
    cursor.execute('''
    SELECT COUNT(*) cnt
    FROM [test].[dbo].[person]
    ''')
    res = cursor.fetchone()
    return res['cnt']

sql = f'''
    SELECT TOP {BATCH} *
    FROM [test].[dbo].[person]
    ORDER BY newid()
    '''

st = time.time()
tot_read = row_cnt = i = 0
row_prev = count_rows()
equal = 0
while True:
    i += 1
    print(f"row_prev: {row_prev}, row_cnt: {row_cnt}")
    time.sleep(1)
    cursor.execute(sql)
    tot_read += len(cursor.fetchall())
    row_cnt = count_rows()
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
print(f"Select {tot_read} rows. {int(vel)} rows per seconds with batch of {BATCH}.")
