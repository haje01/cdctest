import json

from faker import Faker
from faker.providers import internet, date_time, company, phone_number
from sqlalchemy import Table, MetaData, Column, Integer
import pyodbc

with open(snakemake.input[0], 'rt') as f:
    data = json.loads(f.read())

SERVER = data['sqlserver_private_ip']['value']
USER = 'tester'
PASSWD = 'tester@381'
DATABASE = 'test'
constr = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWD}'

cnxn = pyodbc.connect(constr)
cursor = cnxn.cursor()
import pdb; pdb.set_trace()


m = MetaData()
t = Table('t', m,
        Column('id', Integer, primary_key=True),
        Column('x', Integer)
)
m.create_all(engine)

# fake = Faker()
# fake.add_provider(internet)
# fake.add_provider(date_time)
# fake.add_provider(company)
# fake.add_provider(phone_number)

# for i in range(10):
#     print(f"name: {fake.name()}")
#     print(f"address: {fake.address()}")
#     print(f"ip: {fake.ipv4_public()}")
#     print(f"date: {fake.date()}")
#     print(f"company: {fake.company()}")
#     print(f"phone_number: {fake.phone_number()}")