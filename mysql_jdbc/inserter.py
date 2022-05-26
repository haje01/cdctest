import time
import sys
import json
from pathlib import Path

from mysql.connector import connect
from faker import Faker
from faker.providers import internet, date_time, company, phone_number

num_arg = len(sys.argv)
assert num_arg in (2, 5)
dev = num_arg == 2
setup = sys.argv[1]
pid = int(sys.argv[2]) if not dev else -1
epoch = int(sys.argv[3]) if not dev else 100
batch = int(sys.argv[4]) if not dev else 100

with open(setup, 'rt') as f:
    setup = json.loads(f.read())

print(f"Dev: {dev}")
print(f"Epoch: {epoch}")
print(f"Batch: {batch}")

ip = setup['mysql_public_ip'] if dev else setup['mysql_private_ip']
SERVER = ip['value']
USER = setup['db_user']['value']
PASSWD = setup['db_passwd']['value']
DATABASE = 'test'

print(f"{pid} Connect SQL Server at {SERVER}")
conn = connect(host=SERVER, user=USER, password=PASSWD, db=DATABASE)
cursor = conn.cursor()
print("Done")

fake = Faker()
fake.add_provider(internet)
fake.add_provider(date_time)
fake.add_provider(company)
fake.add_provider(phone_number)

st = time.time()
for j in range(epoch):
    print(f"Epoch: {j+1}")
    rows = []
    for i in range(batch):
        row = (
            pid,
            j * batch + i,
            fake.name(),
            fake.address(),
            fake.ipv4_public(),
            fake.date(),
            fake.company(),
            fake.phone_number()
        )
        rows.append(row)
    cursor.executemany("INSERT INTO test.person VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
        rows)
    conn.commit()

conn.close()

elapsed = time.time() - st
vel = epoch * batch / elapsed
print(f"Insert {batch * epoch} rows. {int(vel)} rows per seconds with batch of {batch}.")
