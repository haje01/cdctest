import time
import sys
import json
from pathlib import Path

import pymssql

assert len(sys.argv) > 1
dev = len(sys.argv) == 2
setup = sys.argv[1]
pid = int(sys.argv[2]) if len(sys.argv) > 2 else -1
with open(setup, 'rt') as f:
    tfout = json.loads(f.read())

print(f"Dev: {dev}")
ip = tfout['sqlserver_public_ip'] if dev else tfout['sqlserver_private_ip']
SERVER = ip['value']
# SERVER = tfout['sqlserver_public_ip']['value']
USER = tfout['db_user']['value']
PASSWD = tfout['db_passwd']['value']
DATABASE = 'test'
BATCH = 1000
EPOCH = 100

print(f"{pid} Connect SQL Server at {SERVER}")
conn = pymssql.connect(SERVER, USER, PASSWD, DATABASE)
cursor = conn.cursor(as_dict=True)
print("Done")

sql = f'''
    SELECT TOP {BATCH} *
    FROM [test].[dbo].[person]
    ORDER BY newid()
    '''

while True:
    cursor.execute(sql)
    cursor.fetchall()
    time.sleep(1)

# st = time.time()
# for j in range(EPOCH):
#     print(f"Epoch: {j+1}")
#     cursor.execute(sql)
#     cursor.fetchall()

# conn.close()

# elapsed = time.time() - st
# vel = EPOCH * BATCH / elapsed
# print(f"Total {BATCH * EPOCH} rows, {int(vel)} rows per seconds with batch of {BATCH}.")