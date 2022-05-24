import time
import sys
import json
from pathlib import Path

import pymssql
from faker import Faker
from faker.providers import internet, date_time, company, phone_number

assert len(sys.argv) > 1
dev = len(sys.argv) == 3
deploy = sys.argv[1]
with open(deploy, 'rt') as f:
    tfout = json.loads(f.read())

print(f"Dev: {dev}")
ip = tfout['sqlserver_public_ip'] if dev else tfout['sqlserver_private_ip']
SERVER = ip['value']
# SERVER = tfout['sqlserver_public_ip']['value']
USER = tfout['db_user']['value']
PASSWD = tfout['db_passwd']['value']
DATABASE = 'test'
BATCH = 100
EPOCH = 100

print(f"Connect SQL Server at {SERVER}")
conn = pymssql.connect(SERVER, USER, PASSWD, DATABASE)
cursor = conn.cursor(as_dict=True)
print("Done")

fake = Faker()
fake.add_provider(internet)
fake.add_provider(date_time)
fake.add_provider(company)
fake.add_provider(phone_number)

st = time.time()
for j in range(EPOCH):
    print(f"Epoch: {j+1}")
    rows = []
    for i in range(BATCH):
        row = (
            j * BATCH + i,
            fake.name(),
            fake.address(),
            fake.ipv4_public(),
            fake.date(),
            fake.company(),
            fake.phone_number()
        )
        rows.append(row)
    cursor.executemany("INSERT INTO person VALUES(%d, %s, %s, %s, %s, %s, %s)",
        rows)
    conn.commit()

conn.close()

elapsed = time.time() - st
vel = EPOCH * BATCH / elapsed
print(f"Performance: {int(vel)} rows per seconds with batch of {BATCH}.")