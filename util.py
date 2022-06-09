"""

공용 유틸리티

"""
import time
from pathlib import Path

import pymssql
from mysql.connector import connect
from faker import Faker
from faker.providers import internet, date_time, company, phone_number


def _insert_fake(conn, cursor, epoch, batch, pid):
    fake = Faker()
    fake.add_provider(internet)
    fake.add_provider(date_time)
    fake.add_provider(company)
    fake.add_provider(phone_number)

    for j in range(epoch):
        print(f"Inserter {pid} epoch: {j+1}")
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
        cursor.executemany("INSERT INTO person(pid, sid, name, address, ip, birth, company, phone) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
            rows)
        conn.commit()

