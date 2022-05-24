import time
import sys
import json
from pathlib import Path

import pymssql

num_arg = len(sys.argv)
assert num_arg in (2, 3)
dev = num_arg == 2
setup = sys.argv[1]
pid = int(sys.argv[2]) if not dev else -1
epoch = int(sys.argv[3]) if not dev else -1
pid = int(sys.argv[2]) if not dev else -1


with open(setup, 'rt') as f:
    setup = json.loads(f.read())

print(f"Dev: {dev}")
print(f"Epoch: {epoch}")
print(f"Batch: {batch}")
ip = setup['sqlserver_public_ip'] if dev else setup['sqlserver_private_ip']
SERVER = ip['value']
# SERVER = setup['sqlserver_public_ip']['value']
USER = setup['db_user']['value']
PASSWD = setup['db_passwd']['value']
DATABASE = 'test'

print(f"{pid} Connect SQL Server at {SERVER}")
conn = pymssql.connect(SERVER, USER, PASSWD, DATABASE)
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
    SELECT TOP {10000} *
    FROM [test].[dbo].[person]
    ORDER BY newid()
    '''

st = time.time()
i = 0
prev_cnt = count_rows()
while True:
    i += 1
    time.sleep(1)
    print(f"Epoch: {i}")
    cursor.execute(sql)
    cursor.fetchall()
    cnt = count_rows()
    if cnt == prev_cnt:
        break
    prev_cnt = cnt

conn.close()

elapsed = time.time() - st