import time
from multiprocessing import Process, Queue
import json
from datetime import datetime, timedelta

import pymssql
import pytest
from kafka import KafkaConsumer
import pandas as pd

from kfktest.table import reset_table
from kfktest.util import (count_topic_message, get_kafka_ssh,
    start_kafka_broker, kill_proc_by_port, vm_stop, vm_start,
    restart_kafka_and_connect, stop_kafka_and_connect, count_table_row,
    local_select_proc, local_insert_proc, linfo, NUM_INS_PROCS, NUM_SEL_PROCS,
    remote_insert_proc, remote_select_proc, DB_ROWS, load_setup, insert_fake,
    db_concur, _xjdbc, _hash, ssh_exec, xs3sink, put_connector, unregister_kconn,
    # 픽스쳐들
    xsetup, xcp_setup, xjdbc, xtable, xkafka, xzookeeper, xkvmstart,
    xconn, xkfssh, xdbzm, xrmcons, xcdc, xhash, xtopic
    )


@pytest.fixture(scope="session")
def xprofile():
    return 'mssql'

def test_ct_local_basic(xsetup, xjdbc, xprofile, xkfssh):
    """로컬 insert / select 로 기본적인 Change Tracking 테스트."""
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_SEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INS_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert DB_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_stop(xsetup, xjdbc, xprofile, xkfssh, xhash):
    """카프카 브로커 정상 정지시 Change Tracking 테스트.

    - 기본적으로 Insert / Select 시작 후 브로커를 멈추고 (Stop Gracefully) 다시
        기동해도 메시지 수가 일치해야 한다.
    - 그러나, 커넥터가 메시지 생성 ~ 오프셋 커밋 사이에 죽으면, 재기동시
        커밋하지 않은 오프셋부터 다시 처리하게 되어 메시지 중복이 발생할 수 있다.
    - Graceful 한 정지시 메시지 생성을 정지하고 처리된 오프셋까지만 커밋하면 해결
        가능할 듯 한데..
    - Debezium 도 Exactly Once Semantics 가 아닌 At Least Once 를 지원
    - KIP-618 (Exactly-Once Support for Source Connectors) 에서 이것을 해결하려 함
        - 중복이 없게 하려면 Kafka Connect 를 Transactional Producer 로 구현해야
    - 참고:
        - https://stackoverflow.com/questions/59785863/exactly-once-semantics-in-kafka-source-connector
        - https://camel-context.tistory.com/54

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_SEL_PROCS + 1):
        # select 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INS_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    time.sleep(5)
    # 의존성을 고려해 카프카 브로커와 커넥트 정지
    stop_kafka_and_connect(xprofile, xkfssh, xhash)
    # 의존성을 고려해 카프카 브로커와 커넥트 재개
    restart_kafka_and_connect(xprofile, xkfssh, xhash, False)

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert DB_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_kill(xsetup, xjdbc, xprofile, xkfssh):
    """카프카 브로커 다운시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커 프로세스를 강제로 죽인 후, 잠시 후 다시 재개해도
        메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_SEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INS_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 강제 종료
    time.sleep(7)
    kill_proc_by_port(xkfssh, 9092)
    # 잠시 후 카프카 브로커 start
    time.sleep(10)
    start_kafka_broker(xkfssh)

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=20)
    # 브로커만 강제 Kill 된 경우, 커넥터가 offset 을 flush 하지 못해 다시 시도
    # -> 중복 메시지 발생 가능!
    assert DB_ROWS <= cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_vmstop(xsetup, xjdbc, xprofile, xkfssh):
    """카프카 브로커 VM 정지시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커가 정지 후 재개해도 메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_SEL_PROCS + 1):
        # select 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INS_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 VM 정지 + 재시작
    time.sleep(2)
    vm_stop(xprofile, 'kafka')
    vm_start(xprofile, 'kafka')

    # Reboot 후 ssh 객체 재생성 필요!
    kfssh = get_kafka_ssh(xprofile)
    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=20)
    assert DB_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_db(xcp_setup, xprofile, xkfssh, xtable):
    """DB 기본성능 확인을 위해 원격 insert / select 만 수행."""
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_SEL_PROCS + 1):
        # select 프로세스
        p = Process(target=remote_select_proc, args=(xprofile, xcp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INS_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_insert_proc, args=(xprofile, xcp_setup, pid))
        ins_pros.append(p)
        p.start()

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")

    # 테이블 행수 확인
    cnt = count_table_row(xprofile)
    assert DB_ROWS  == cnt


def test_ct_remote_basic(xcp_setup, xjdbc, xprofile, xkfssh):
    """원격 insert / select 로 기본적인 Change Tracking 테스트.

    - Inserter / Selector 출력은 count 가 끝난 뒤 몰아서 나옴.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_SEL_PROCS + 1):
        # select 프로세스
        p = Process(target=remote_select_proc, args=(xprofile, xcp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INS_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_insert_proc, args=(xprofile, xcp_setup, pid))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert DB_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_cdc_remote_basic(xcp_setup, xdbzm, xprofile, xkfssh, xtable):
    """원격 insert / select 로 기본적인 Change Data Capture 테스트.

    - 테스트 시작전 이전 토픽을 참고하는 것이 없어야 함. (delete_topic 에러 발생)
    - Inserter / Selector 출력은 count 가 끝난 뒤 몰아서 나옴.

    """

    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_SEL_PROCS + 1):
        # select 프로세스
        p = Process(target=remote_select_proc, args=(xprofile, xcp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INS_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_insert_proc, args=(xprofile, xcp_setup, pid))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'db1.dbo.person', timeout=10)
    assert DB_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")

    linfo(f"CDC Test Elapsed: {time.time() - xtable:.2f}")


def test_ct_modify(xcp_setup, xjdbc, xtable, xprofile, xkfssh):
    """CT 방식에서 기존 행이 변하는 경우 동작 확인.

    - CT 방식은 기존행이 변경 (update) 된 것은 전송하지 않는다.
    - 새 행이 추가된 것은 잘 보냄
    - 테이블 초기화 후 새로 insert 하면 ID 가 같은 것은 기존 데이터를 유지하고
      새 ID 의 데이터는 가져옴

    """
    num_msg = 10
    # Insert 프로세스들 시작
    ins_pros = []
    # insert 프로세스
    p = Process(target=local_insert_proc, args=(xprofile, 1, 1, num_msg))
    ins_pros.append(p)
    p.start()

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    setup = load_setup('mssql')
    topic = 'mssql-person'
    broker_addr = setup['kafka_public_ip']['value']
    broker_port = 19092

    def consume():
        consumer = KafkaConsumer(topic,
                        group_id=f'my-group-mssql',
                        bootstrap_servers=[f'{broker_addr}:{broker_port}'],
                        auto_offset_reset='earliest',
                        value_deserializer=lambda x: json.loads(x.decode('utf8')),
                        enable_auto_commit=False,
                        consumer_timeout_ms=1000 * 10
                        )
        return consumer

    cnt = 0
    frow = org_name = None
    for _msg in consume():
        msg = _msg.value
        cnt += 1
        if msg['id'] == 1:
            org_name = msg['name']
    assert cnt == num_msg

    # 일부 행을 변경
    db_host = setup['mssql_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']['result']
    db_name = 'test'

    conn = pymssql.connect(host=db_host, user=db_user, password=db_passwd, database=db_name)
    cursor = conn.cursor()
    linfo("Connect done.")
    sql = "UPDATE person SET name='MODIFIED' WHERE id=1"
    cursor.execute(sql)
    conn.commit()

    cnt = mcnt = 0
    fname = None
    for _msg in consume():
        msg = _msg.value
        cnt += 1
        if msg['id'] == 1:
            fname = msg['name']
            assert fname == org_name
            mcnt += 1
    assert mcnt == 1

    # 새 행을 추가
    insert_fake(conn, cursor, 1, 1, 1, 'mssql')
    conn.close()

    cnt = mcnt = 0
    for _msg in consume():
        msg = _msg.value
        cnt += 1
        if msg['id'] == 1:
            mcnt += 1
    assert cnt == 11
    assert mcnt == 1

    # 테이블을 리셋 후 행 추가 (rotation 흉내)
    conn, cursor = reset_table(xprofile)
    insert_fake(conn, cursor, 1, 15, 1, 'mssql')

    cnt = 0
    for _msg in consume():
        msg = _msg.value
        cnt += 1
        if msg['id'] == 1:
            # 같은 ID 에 대해서는 기존 값이 그대로 옴
            assert fname == msg['name']
    # 새로 추가된 행은 들어옴
    assert cnt == 15


@pytest.mark.parametrize('xjdbc', [{'inc_col': 'id', 'ts_col': 'regdt'}], indirect=True)
def test_ct_modify2(xcp_setup, xjdbc, xtable, xprofile, xkfssh):
    """CT 방식에서 Current Timestamp 를 쓸 때 기존 행이 변하는 경우 동작 확인.

    - Incremental 과 Timestamp 컬럼을 함께 쓰는 경우
    - update 를 하면서 Timestamp 컬럼을 갱신하면 커넥터는 그 행을 다시 가져온다.
    - 토픽에는 기존에 가져온 행 + 다시 가져온 행의 메시지가 있게 됨

    """
    num_msg = 10
    # Insert 프로세스들 시작
    ins_pros = []
    # insert 프로세스
    p = Process(target=local_insert_proc, args=(xprofile, 1, 1, num_msg))
    ins_pros.append(p)
    p.start()

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    setup = load_setup('mssql')
    topic = 'mssql-person'
    broker_addr = setup['kafka_public_ip']['value']
    broker_port = 19092

    def consume():
        consumer = KafkaConsumer(topic,
                        group_id=f'my-group-mssql',
                        bootstrap_servers=[f'{broker_addr}:{broker_port}'],
                        auto_offset_reset='earliest',
                        value_deserializer=lambda x: json.loads(x.decode('utf8')),
                        enable_auto_commit=False,
                        consumer_timeout_ms=1000 * 10
                        )
        return consumer

    cnt = 0
    frow = org_name = None
    for _msg in consume():
        msg = _msg.value
        cnt += 1
        if msg['id'] == 1:
            org_name = msg['name']
    assert cnt == num_msg

    # 일부 행을 변경
    db_host = setup['mssql_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']['result']
    db_name = 'test'

    conn = pymssql.connect(host=db_host, user=db_user, password=db_passwd, database=db_name)
    cursor = conn.cursor()
    linfo("Connect done.")
    sql = "UPDATE person SET regdt=CURRENT_TIMESTAMP, name='MODIFIED' WHERE id=1"
    cursor.execute(sql)
    conn.commit()

    time.sleep(10)

    cnt = mcnt = 0
    fnames = []
    for _msg in consume():
        msg = _msg.value
        cnt += 1
        if msg['id'] == 1:
            fname = msg['name']
            fnames.append(fname)
    assert len(fnames) == 2
    assert 'MODIFIED' in fnames
    assert cnt == 11


def yesterday():
    return (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

#
# 어제 날짜 테이블이 있으면 그것과 당일을 UNION
# 아니면, 당일만 SELECT 하는 쿼리
# 그러나, 아래와 같은 이유로 이런 방식은 쓸 수 없다.. ㅠㅠ
#   - 커넥터에서 WHERE 조건을 추가한다
#   - MSSQL 에서 Dynamic SQL 을 Subquery 처럼 쓸 수 없다.
#
ctq_query = """
DECLARE @_today DATETIME = GETDATE()

---- 하루 간격 로테이션
-- DECLARE @_prev DATETIME = DATEADD(day, -1, CAST(GETDATE() AS date))
-- DECLARE @prev NCHAR(8) = FORMAT(DATEADD(day, -1, @_today), 'yyyyMMdd')
---- 일분 간격 로테이션
DECLARE @prev NCHAR(6) = FORMAT(DATEADD(minute, -1, @_today), 'ddHHmm')

DECLARE @tblName NCHAR(30) = N'person'
DECLARE @ptblName NCHAR(30) = N'person_' + @prev

DECLARE @query NVARCHAR(4000)

SET @query = '
IF EXISTS ( SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''' + @ytblName + ''' )
BEGIN
    SELECT * FROM (
        SELECT * FROM ' + @ytblName + ' WHERE _JSCOND
        UNION ALL
        SELECT * FROM ' + @tblName + ' WHERE _JSCOND
    ) AS T
    -- WHERE "id" > ? ORDER BY "id" ASC (io.confluent.connect.jdbc.source.TableQuerier:179)
END
ELSE
	SELECT * FROM ' + @tblName + ' WHERE _JSCOND'

SELECT *
EXEC sp_executesql @query
"""

# ctq_query = """
# SELECT * FROM person
# """

# @pytest.mark.parametrize('xtable', [{'tables':
#    [f'person', f"person_{yesterday()}"]}], indirect=True)
@pytest.mark.parametrize('xjdbc', [{
        'inc_col': 'id',
        'query': ctq_query,
        'query_topic': 'mssql-person'  # 대상 토픽 이름 명시
        }], indirect=True)
def test_ct_query(xcp_setup, xjdbc, xtable, xprofile, xkfssh):
    """CT 방식에서 테이블이 아닌 쿼리로 가져오기."""
    num_msg = 5

    # Insert 프로세스들 시작
    ins_pros = []
    # 당일 데이터
    p = Process(target=local_insert_proc, args=(xprofile, 1, 1, num_msg, False,
            f'person'))
    ins_pros.append(p)
    p.start()

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert num_msg == cnt

# 대상 날짜는 Snakemake batch_fake 에서 만든 테이블의 그것과 일치해야 한다!
ctm_dates = pd.date_range(start='20220801', end='20220831')
# 대상 테이블명
ctm_tables = [dt.strftime('person_%Y%m%d') for dt in ctm_dates]
# 대상 토픽명
ctm_topics = [f'mssql-{t}' for t in ctm_tables]
@pytest.mark.parametrize('xjdbc', [{'inc_col': 'id', 'tasks': 1}],indirect=True)
# 테이블명에 해당하는 토픽 리셋
@pytest.mark.parametrize('xtopic', [{'topics': ctm_topics}], indirect=True)
@pytest.mark.parametrize('xtable', [{'skip': True}], indirect=True)
def test_ct_multitable(xcp_setup, xjdbc, xtable, xprofile, xtopic, xkfssh):
    """여러 테이블을 대상으로 할 때 퍼포먼스 테스트.

    다음과 같은 목적:
    - 주기적으로 로테이션되는 테이블은 타겟 설정이 곤란
    - 이 경우 whitelist 없이 DB 를 타겟으로 해두면 생성되는 모든 테이블이 타겟
    - 이렇게 하면 잦은 설정 변경이나 query 없이 로테이션에 대응되나,
      많은 테이블이 대상이 되면 퍼포먼스 확인 필요

    퍼포먼스 확인:
    - 필요한 개수가 되도록 일별 대상 테이블을 snakemake 의 batch_fake 로 만들어 둠
    - 테스트 실행 초기 모든 테이블에 대해 커넥터가 가져올 때 부하가 많이 걸릴 것
    - 초기 데이터 가져오기가 끝난 후 DB 장비의 CPU 부하를 모니터링
    - 키를 눌러 추가 데이터 가져오기 확인

    실행 조건:
    - 가짜 데이터 테이블들을 snakemake 의 batch_fake 로 미리 만둘어 두어야 함.

    """

    # 시작하자 마자 Connect 동작 -> DB 장비의 상태를 확인

    input("\n=== Press Enter key to check topic messages ===\n")

    # 모든 토픽의 메시지 수 확인
    total = count_topic_message(xprofile, ','.join(ctm_topics))
    assert 100000 * len(ctm_topics) == total

    input("\n=== Press enter key to insert new data ===\n")

    conn, cursor = db_concur('mssql')
    # 최근 날짜에 새 데이터 인서트하며 DB 상태 확인
    insert_fake(conn, cursor, 1, 10000, 1, 'mssql', table=ctm_tables[-1])


CTR_MAX_ROTATION = 1
CTR_INSERTS = 65  # CTR_MAX_ROTATION 번 로테이션에 충분한 메시지 인서트
CTR_BATCH = 1

def ctr_insert_proc():
    """130 초 동안 초당 1 번 insert."""
    linfo(f"[ ] ctr_insert_proc")
    conn, cursor = db_concur('mssql')

    for i in range(CTR_INSERTS):
        # pid 를 메시지 ID 로 대용
        insert_fake(conn, cursor, 1, CTR_BATCH, i, 'mssql', table='person')
        time.sleep(1)
    linfo(f"[v] ctr_insert_proc")


def ctr_rotate_proc():
    """분당 한 번 테이블 로테이션."""
    linfo(f"[ ] ctr_rotate_proc")
    conn, cursor = db_concur('mssql')
    cnt = 0
    prev = None
    while True:
        now = datetime.now()
        thisdt = now.strftime('%d%H%M')
        # 분이 바뀌었으면 로테이션
        if prev is not None and thisdt != prev:
            linfo(f"== rotate person to person_{prev} ==")
            sql = f"""
                exec sp_rename 'person', person_{prev};
                CREATE TABLE person (
                    id int IDENTITY(1,1) PRIMARY KEY,
                    regdt DATETIME2 DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    pid INT NOT NULL,
                    sid INT NOT NULL,
                    name VARCHAR(40),
                    address VARCHAR(200),
                    ip VARCHAR(20),
                    birth DATE,
                    company VARCHAR(40),
                    phone VARCHAR(40)
                );
            """
            cursor.execute(sql)
            conn.commit()
            cnt += 1
            # 최대 로테이션 수 체크
            if cnt >= CTR_MAX_ROTATION:
                break
        prev = thisdt
        time.sleep(1)

    linfo(f"[v] ctr_rotate_proc")


def ctr_register_proc(setup, param, chash):
    """분당 한 번 쿼리 변경후 등록."""
    linfo(f"[ ] ctr_register_proc {chash}")
    ssh = get_kafka_ssh('mssql')
    prevdt = None
    cnt = 0
    while True:
        now = datetime.now()
        thisdt = now.strftime('%d%H%M')
        # 분이 바뀌었으면 쿼리 재등록
        if prevdt is not None and thisdt != prevdt:
            # rotation 이 먼저 되어야 하기에 조금 늦게
            time.sleep(3)
            linfo(f"[ ] == register new query with person_{prevdt} ==")

            cname = f'jdbc-mssql-{chash}'
            for i in range(1):
                # 기존 커넥터 제거
                unregister_kconn(ssh, cname)

                # 새 커넥터 등록
                linfo(f"[ ] register new connector")
                param['query'] = ctr_query.format(prevdt)
                _xjdbc('mssql', setup, ssh, chash, param)
                linfo(f"[v] register new connector")
                linfo(f"[v] == register new query with person_{prevdt} ==")

            cnt += 1
            # 최대 로테이션 수 체크
            if cnt >= CTR_MAX_ROTATION:
                break
        prevdt = thisdt
        time.sleep(1)
    linfo(f"[v] ctr_register_proc {chash}")

ctr_now = datetime.now()
ctr_prev = (ctr_now - timedelta(minutes=1)).strftime('%d%H%M')
ctr_hash = _hash()
# @pytest.mark.parametrize('xjdbc', [ctr_param], indirect=True)
# def test_ct_rotquery(xcp_setup, xjdbc, xs3sink, xtable, xprofile, xtopic, xkfssh):
#     """로테이션되는 테이블을 쿼리로 가져오기.

#     동기:
#     - MSSQL 에서 로테이션되는 테이블을 CT 방식으로 가져오는 경우
#     - 현재 테이블과 전 테이블을 가져오는 방식은 각각의 토픽이 되기에 문제
#       - 전 테이블의 모든 메시지를 다시 가져옴
#     - 쿼리 방식은 쿼리가 바뀌어도 하나의 토픽에 저장가능

#     테스트 구현:
#     - 이번 테이블과 전 테이블을 가져오는 쿼리 작성
#       - JDBC 커넥터가 이 쿼리에 가져온 ID 중복 체크용 WHERE 절을 추가하게 됨
#       - MSSQL 의 Dynamic SQL 을 이용하면 편리하나, 이경우 커넥터가 붙이는 WHERE 절과 맞지 않음
#     - 이 쿼리는 로테이션 간격마다 Cron Job 등으로 대상 테이블 변경
#     - 기본적으로 모든 메시지는 person 테이블에 추가되고,
#       - 로테이션된 테이블에 일부 가져오지 못한 짜투리 메시지 남을 가능성
#     - 로그 생성기는 별도 프로세스로 지속
#     - 또 다른 프로세스에서 샘플 로그 테이블 1분 간격으로 로테이션
#         예) 현재 11 일 0시 0분 1초인 경우
#         person (현재), person_102359 (전날 23시 59분까지 로테이션)
#     - 커넥터를 바꾸는 동안 잠시 두 개의 커넥터가 공존하게 되고 이때 메시지 중복이 생길 수 있다.
#       - At Least Once

#     결과:
#         - 기존 커넥터 제거 후 커넥터의 이름을 바꾸지 않고 다시 등록할 때는 이따금씩 메시지 손실 발생
#         - 같은 이름의 커넥터가 있는 상태에서 또 등록하는 것은 에러 발생
#         - 기존 커넥터를 먼저 제거하고 다른 이름으로 커넥터를 등록하면, 전체 메시지를 새로 받음

#     """
#     # 시작시 이전 테이블을 초기 에러 방지를 위해 만들어 주기
#     reset_table('mssql', table=f'person_{ctr_prev}')

#     # insert 프로세스 시작
#     pi = Process(target=ctr_insert_proc)
#     pi.start()

#     # rotation 프로세스 시작
#     pr = Process(target=ctr_rotate_proc)
#     pr.start()

#     # register 프로세스 시작
#     pq = Process(target=ctr_register_proc, args=(xcp_setup, ctr_param, ctr_hash))
#     pq.start()

#     pi.join()
#     pr.join()
#     pq.join()

#     time.sleep(7)

#     # 토픽 메시지는 적어도 DB 행 수보다 크거나 같아야 한다 (At Least Once)
#     count = count_topic_message('mssql', 'mssql-person')
#     linfo(f"Orignal Messages: {CTR_INSERTS * CTR_BATCH}, Topic Messages: {count}")

#     # 빠진 ID 가 없는지 확인
#     cmd = "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mssql-person --from-beginning --timeout-ms 3000"
#     ssh = get_kafka_ssh('mssql')
#     ret = ssh_exec(ssh, cmd)
#     pids = set()
#     for line in ret.split('\n'):
#         try:
#             data = json.loads(line)
#         except json.decoder.JSONDecodeError:
#             print(line)
#             continue
#         pid = data['pid']
#         pids.add(pid)
#     missed = set(range(CTR_INSERTS)) - pids
#     linfo(f"Missing message pids: {missed}")


# ctr_query2 = """
# DECLARE @_today DATETIME = GETDATE()

# ---- 하루 간격 로테이션
# -- DECLARE @_prev DATETIME = DATEADD(day, -1, CAST(GETDATE() AS date))
# -- DECLARE @prev NCHAR(8) = FORMAT(DATEADD(day, -1, @_today), 'yyyyMMdd')
# ---- 일분 간격 로테이션
# DECLARE @prev NCHAR(6) = FORMAT(DATEADD(minute, -1, @_today), 'ddHHmm')

# DECLARE @tblName NCHAR(30) = N'person'
# DECLARE @ptblName NCHAR(30) = N'person_' + @prev

# DECLARE @query NVARCHAR(4000)

# SET @query = '
# SELECT * FROM (
#     SELECT * FROM ' + @ptblName + '
#     UNION ALL
#     SELECT * FROM ' + @tblName + '
# ) AS T
# WHERE _JSCOND
# EXEC sp_executesql @query
# """

# 테스트 시작 후 분이 바뀔 때까지의 시간을 구해 매크로의 지연 시간으로 사용
ctr_now = datetime.now()
ctr_next = ctr_now + timedelta(seconds=60)
ctr_delay = (datetime(ctr_next.year, ctr_next.month, ctr_next.day, ctr_next.hour, ctr_next.minute) - ctr_now).seconds + 5
ctr_query = f"""
SELECT * FROM (
    -- 1분 단위 테이블 로테이션
    SELECT * FROM person_{{{{ MinAddFmtDelay -1 ddHHmm {ctr_delay} }}}}
    UNION ALL
    SELECT * FROM person
) AS T
"""
ctr_param = {
        'query': ctr_query,
        'query_topic': 'mssql-person',
        'inc_col': 'pid',  # 로테이션이 되어도 유니크한 컬럼으로
        'chash': _hash()
    }
@pytest.mark.parametrize('xjdbc', [ctr_param], indirect=True)
def test_ct_rtbl_inc(xcp_setup, xjdbc, xtable, xprofile, xtopic, xkfssh):
    """로테이션되는 테이블을 Dynamic SQL 쿼리로 가져오기.

    Incremental 컬럼만 이용하는 경우 경우

    동기:
    - MSSQL 에서 로테이션되는 테이블을 CT 방식으로 가져오는 경우
    - 현재 테이블과 전 테이블을 가져오는 방식은 각각의 토픽이 되기에 문제
      - 전 테이블의 모든 메시지를 다시 가져옴
    - 쿼리 방식은 쿼리가 바뀌어도 하나의 토픽에 저장가능

    테스트 구현:
    - 원래는 MSSQL 의 Dynamic SQL 을 이용해 이번 테이블과 전 테이블을 가져오는 쿼리를 작성하려 했으나
    - JDBC 커넥터에서 쿼리 끝에 자체적인 WHERE 조건을 붙여주기에 Dynamic SQL 을 사용할 수 없음
    - 쿼리에 매크로를 지원하는 수정된 JDBC 커넥터 (github.com/haje01/kafka-connect-jdbc) 를 이용
    - 로그 생성기는 별도 프로세스로 지속
    - 또 다른 프로세스에서 샘플 로그 테이블 1분 간격으로 로테이션
        예) 현재 11 일 0시 0분 1초인 경우
        person (현재), person_102359 (전날 23시 59분까지 로테이션)

    """
    # 시작시 이전 테이블을 초기 에러 방지를 위해 만들어 주기
    reset_table('mssql', table=f'person_{ctr_prev}')

    # insert 프로세스 시작
    pi = Process(target=ctr_insert_proc)
    pi.start()

    # rotation 프로세스 시작
    pr = Process(target=ctr_rotate_proc)
    pr.start()

    pi.join()
    pr.join()

    time.sleep(7)

    # 토픽 메시지 수는 DB 행 수는 크거나 같아야 한다
    count = count_topic_message('mssql', 'mssql-person')
    assert CTR_INSERTS * CTR_BATCH <= count
    linfo(f"Orignal Messages: {CTR_INSERTS * CTR_BATCH}, Topic Messages: {count}")

    # 빠진 ID 가 없는지 확인
    cmd = "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mssql-person --from-beginning --timeout-ms 3000"
    ssh = get_kafka_ssh('mssql')
    ret = ssh_exec(ssh, cmd)
    pids = set()
    for line in ret.split('\n'):
        try:
            data = json.loads(line)
        except json.decoder.JSONDecodeError:
            print(line)
            continue
        pid = data['pid']
        pids.add(pid)
    missed = set(range(CTR_INSERTS)) - pids
    linfo(f"Check missing messages: {missed}")
    assert len(missed) == 0


ctr2_param = {
        'query': ctr_query,
        'query_topic': 'mssql-person',
        'inc_col': 'id',
        'ts_col': 'regdt',
        'chash': _hash()
    }
@pytest.mark.parametrize('xjdbc', [ctr2_param], indirect=True)
def test_ct_rtbl_incts(xcp_setup, xjdbc, xs3sink, xtable, xprofile, xtopic, xkfssh):
    """로테이션되는 테이블을 Dynamic SQL 쿼리로 가져오기.

    Incremental + Timestamp 컬럼 이용하는 경우

    동기:
    - MSSQL 에서 로테이션되는 테이블을 CT 방식으로 가져오는 경우
    - 현재 테이블과 전 테이블을 가져오는 방식은 각각의 토픽이 되기에 문제
      - 전 테이블의 모든 메시지를 다시 가져옴
    - 쿼리 방식은 쿼리가 바뀌어도 하나의 토픽에 저장가능

    테스트 구현:
    - 원래는 MSSQL 의 Dynamic SQL 을 이용해 이번 테이블과 전 테이블을 가져오는 쿼리를 작성하려 했으나
    - JDBC 커넥터에서 쿼리 끝에 자체적인 WHERE 조건을 붙여주기에 Dynamic SQL 을 사용할 수 없음
    - 쿼리에 매크로를 지원하는 수정된 JDBC 커넥터 (github.com/haje01/kafka-connect-jdbc) 를 이용
    - 로그 생성기는 별도 프로세스로 지속
    - 또 다른 프로세스에서 샘플 로그 테이블 1분 간격으로 로테이션
        예) 현재 11 일 0시 0분 1초인 경우
        person (현재), person_102359 (전날 23시 59분까지 로테이션)

    """
    # 시작시 이전 테이블을 초기 에러 방지를 위해 만들어 주기
    reset_table('mssql', table=f'person_{ctr_prev}')

    # insert 프로세스 시작
    pi = Process(target=ctr_insert_proc)
    pi.start()

    # rotation 프로세스 시작
    pr = Process(target=ctr_rotate_proc)
    pr.start()

    pi.join()
    pr.join()

    time.sleep(7)

    # 토픽 메시지 수는 DB 행 수는 크거나 같아야 한다
    count = count_topic_message('mssql', 'mssql-person')
    assert CTR_INSERTS * CTR_BATCH <= count
    linfo(f"Orignal Messages: {CTR_INSERTS * CTR_BATCH}, Topic Messages: {count}")

    # 빠진 ID 가 없는지 확인
    cmd = "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mssql-person --from-beginning --timeout-ms 3000"
    ssh = get_kafka_ssh('mssql')
    ret = ssh_exec(ssh, cmd)
    pids = set()
    for line in ret.split('\n'):
        try:
            data = json.loads(line)
        except json.decoder.JSONDecodeError:
            print(line)
            continue
        pid = data['pid']
        pids.add(pid)
    missed = set(range(CTR_INSERTS)) - pids
    linfo(f"Check missing messages: {missed}")
    assert len(missed) == 0

