import pdb
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
    local_consume_proc, ssh_exec, inserter_kill_processes,
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
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
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
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
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
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=20)
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
    cnt = count_topic_message(kfssh, f'{xprofile}-person', timeout=20)
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
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
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
    cnt = count_topic_message(xkfssh, f'db1.dbo.person', timeout=10)
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
        msg = _msg.value['payload']
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
        msg = _msg.value['payload']
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
        msg = _msg.value['payload']
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
        msg = _msg.value['payload']
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
        msg = _msg.value['payload']
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
        msg = _msg.value['payload']
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
# 그러나, 이런 방식은 쓸 수 없다.. ㅠㅠ
#   - 커넥터에서 WHERE 조건을 건다
#   - MSSQL 에서 Dynamic SQL 을 Subquery 처럼 쓸 수 없다.
#
# query = """
# DECLARE @_today DATETIME = GETDATE()
# DECLARE @_yesterday DATETIME = DATEADD(day, -1, CAST(GETDATE() AS date))
# DECLARE @today NCHAR(8) = CONVERT(NCHAR(8), @_today, 112)
# DECLARE @yesterday NCHAR(8) = CONVERT(NCHAR(8), @_yesterday, 112)

# DECLARE @tblName NCHAR(30) = N'person'
# DECLARE @ytblName NCHAR(30) = N'person_' + @yesterday

# DECLARE @query NVARCHAR(4000)
# SET @query = '
# IF EXISTS ( SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''' + @ytblName + ''' )
# BEGIN
#     SELECT * FROM (
#         SELECT * FROM ' + @ytblName + '
#         UNION ALL
#         SELECT * FROM ' + @tblName + '
#     ) AS T
#      WHERE "id" > ? ORDER BY "id" ASC (io.confluent.connect.jdbc.source.TableQuerier:179)
# END
# ELSE
# 	SELECT * FROM ' + @tblName

# SELECT *
# EXEC sp_executesql @query
# """

ctq_query = """
SELECT * FROM person
"""

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
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
    assert num_msg == cnt

# 대상 날짜 (3개월 = 2022-06-01 부터 2022-08-31 까지)
ctm_dates = pd.date_range(start='20220601', end='20220831')
# ctm_dates = pd.date_range(start='20220801', end='20220803')
# 대상 테이블명
ctm_tables = [dt.strftime('person_%Y%m%d') for dt in ctm_dates]
# 대상 토픽명
ctm_topics = [f'mssql-{t}' for t in ctm_tables]
@pytest.mark.parametrize('xtable', [{'tables': ctm_tables}], indirect=True)
# 대상 DB 내 모든 테이블을 대상으로 하도록 커넥터에는 테이블 정보를 주지 않음
@pytest.mark.parametrize('xjdbc', [{'inc_col': 'id', 'tasks': 1}],indirect=True)
# 테이블명에 해당하는 토픽 리셋
@pytest.mark.parametrize('xtopic', [{'topics': ctm_topics}], indirect=True)
def test_ct_multitable(xcp_setup, xjdbc, xtable, xprofile, xtopic, xkfssh):
    """여러 테이블을 대상으로 할 때 퍼포먼스 테스트.

    - 주기적으로 로테이션되는 테이블은 타겟 설정이 곤란
    - 이 경우 whitelist 없이 DB 를 타겟으로 해두면 생성되는 모든 테이블이 타겟
    - 이렇게 하면 잦은 설정 변경이나 query 없이 로테이션에 대응되나,
      많은 테이블이 대상이 되면 퍼포먼스 확인 필요

    퍼포먼스 확인
    - 필요한 개수가 되도록 일별 대상 테이블 날짜 범위 지정
    - 테스트 실행 초기 토픽 리셋 및 더미 데이터 인서트 단계에서 시간이 많이 걸림!
    - 인서트가 끝나고 커넥터가 테이블을 다 가져온 후,
    - 아이들 상태에서 DB 장비의 CPU 부하를 모니터링

    """
    # DB 에 더미 데이터를 넣는 동안 Kafka Connector 잠시 중단
    ssh_exec(xkfssh, "sudo service kafka-connect stop")

    # def chunks(lst, n):
    #     """Yield successive n-sized chunks from lst."""
    #     for i in range(0, len(lst), n):
    #         yield lst[i:i + n]

    # Insert 부하를 줄이기 위해 대상 테이블 10개 단위로 쪼개기 (DB 커넥션 수도 문제?)
    # table_chunks = chunks(ctm_tables, 10)

    num_epoch = 10
    num_batch = 10000
    num_rows = num_epoch * num_batch

    # 인서트
    def insert(remote, tables):
        if remote:
            # 원격 insert 는 모든 테이블명을 건내주어 그곳에서 병렬처리
            tables = ','.join(tables)
            ret = remote_insert_proc(xprofile, xcp_setup, 1, num_epoch, num_batch,
                                    False, tables)
        else:
            # 로컬 insert 는 프로세스 여럿 이용
            ins_pros = []
            for pid, table in enumerate(tables):
                p = Process(target=local_insert_proc, args=(xprofile, pid, num_epoch,
                            num_batch, False, table))
                ins_pros.append(p)
                p.start()
            for p in ins_pros:
                p.join()

    # 원격 Insert 여부
    insert_remote = True
    # 프로세스당 10만 행의 경우 인서트에 걸린 시간
    #   로컬 : 약 1058 Rows Per Seconds
    #   원격 : 약 3000 Rows Per Seconds

    # 원격 insert 의 경우 기존에 종료되지 않은 프로세스가 있으면 중단
    if insert_remote:
        inserter_kill_processes(xprofile, xcp_setup, 'python3 -m kfktest.inserter mssql.*')

    st = time.time()
    #
    # Insert 시작
    #
    insert(insert_remote, ctm_tables)

    elapsed = time.time() - st
    linfo(f"All insert processes are done in {elapsed:.2f} sec.")
    linfo(f"Average speed {(num_rows * len(ctm_tables)) / elapsed:.1f} RPS.")

    input("\n=== Press Enter key to start consume ===\n")

    # Kafka Connector 시작
    ssh_exec(xkfssh, "sudo service kafka-connect start")

    # 모든 토픽의 메시지 수 확인
    cnt_procs = []
    cnt_qs = []
    for pid, topic in enumerate(ctm_topics):
        q = Queue()
        p = Process(target=local_consume_proc, args=(xprofile, pid, q, topic, 20))
        cnt_procs.append(p)
        cnt_qs.append(q)
        p.start()

    total = 0
    for i, p in enumerate(cnt_procs):
        cnt = cnt_qs[i].get()
        print(f'cnt_proc {i} cnt {cnt}')
        total += cnt
        p.join()

    assert num_rows * len(ctm_topics) == total

    input("\n=== Press enter key to insert new data ===\n")

    # 최근 날짜에 새 데이터 인서트하며 DB 상태 확인
    insert(insert_remote, [ctm_tables[-1]])