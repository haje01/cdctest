import time
import sys
import json
from pathlib import Path

import pymssql
from faker import Faker
from faker.providers import internet, date_time, company, phone_number

from util import mssql_insert_fake

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

mssql_insert_fake(setup, epoch, batch, pid)
