"""

MySQL 용 유틸리티

"""
import time
import sys
import json
from pathlib import Path

from mysql.connector import connect
from faker import Faker
from faker.providers import internet, date_time, company, phone_number


def insert_fake(setup, epoch, batch, pid=0):
    """가짜 데이터 인서트.

    Args:
        setup (dict): 인프라 설치 정보
        epoch (int): 에포크 수
        batch (int): 에포크당 배치 수
        pid (str): 멀티 프로세스 인서트시 구분용 ID

    """
    db_host = setup['mysql_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']
    db_name = 'test'

    print(f"Inserter {pid} connect SQL Server at {db_host}")
    conn = connect(host=db_host, user=db_user, password=db_passwd, db=db_name)
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
        cursor.executemany("INSERT INTO test.person(pid, sid, name, address, ip, birth, company, phone) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
            rows)
        conn.commit()

    conn.close()

    elapsed = time.time() - st
    vel = epoch * batch / elapsed
    print(f"Insert {batch * epoch} rows. {int(vel)} rows per seconds with batch of {batch}.")
