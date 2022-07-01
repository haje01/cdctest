import os
import time
from multiprocessing import Process

import pytest
from mysql.connector import connect

from kfktest.util import (SSH, count_topic_message, ssh_exec, stop_kafka_broker,
    start_kafka_broker, kill_proc_by_port, vm_start, vm_stop, vm_hibernate,
    get_kafka_ssh, stop_kafka_and_connect, restart_kafka_and_connect, linfo,
    count_table_row, DB_PRE_ROWS, NUM_INSEL_PROCS, local_insert_proc,
    local_select_proc, remote_insert_proc, remote_select_proc, DB_ROWS,
    # 픽스쳐들
    xsetup, xjdbc, xcp_setup, xtable, xtopic_ct, xkafka, xzookeeper, xkvmstart,
    xconn, xkfssh, xdbzm, xrmcons, xtopic_cdc, xhash, xcdc
    )


@pytest.fixture(scope="session")
def xprofile():
    return 'mysql'


def test_ct_local_basic(xjdbc, xkfssh, xsetup, xprofile):
    """로컬 insert / select 로 기본적인 Change Tracking 테스트.

    - 테스트 시작전 이전 토픽을 참고하는 것이 없어야 함. (delete_topic 에러 발생)

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_stop(xsetup, xjdbc, xkfssh, xprofile, xhash):
    """카프카 브로커 정상 정지시 Change Tracking 테스트.

    - 기본적으로 Insert / Select 시작 후 브로커를 멈추고 (Stop Gracefully) 다시
        기동해도 메시지 수가 일치해야 한다.
    - 그러나, 커넥터가 메시지 생성 ~ 오프셋 커밋 사이에 죽으면, 재기동시
        커밋하지 않은 오프셋부터 다시 처리하게 되어 메시지 중복이 발생할 수 있다.
    - Debezium 도 Exactly Once Semantics 가 아닌 At Least Once 를 지원
    - KIP-618 (Exactly-Once Support for Source Connectors) 에서 이것을 해결하려 함
        - 중복이 없게 하려면 Kafka Connect 를 Transactional Producer 로 구현해야
    - 참고:
        - https://stackoverflow.com/questions/59785863/exactly-once-semantics-in-kafka-source-connector
        - https://camel-context.tistory.com/54

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
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
    # 정지 시점에 따라 중복 발생 가능
    assert DB_ROWS + DB_PRE_ROWS <= cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_kill(xsetup, xjdbc, xkfssh, xprofile):
    """카프카 브로커 다운시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커 프로세스를 강제로 죽인 후, 잠시 후 다시 재개해도
        메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 강제 종료
    time.sleep(1)
    kill_proc_by_port(xkfssh, 9092)
    # 잠시 후 카프카 브로커 start
    time.sleep(1)
    start_kafka_broker(xkfssh)

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
    # 브로커만 강제 Kill 된 경우, 커넥터가 offset 을 flush 하지 못해 다시 시도
    # -> 중복 메시지 발생 가능!
    assert DB_ROWS + DB_PRE_ROWS <= cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_vmstop(xsetup, xjdbc, xkfssh, xprofile):
    """카프카 브로커 VM 정지시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커가 정지 후 재개해도 메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
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
    cnt = count_topic_message(kfssh, f'{xprofile}-person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


@pytest.mark.skip(reason="시간이 많이 걸림.")
def test_ct_broker_hibernate(xsetup, xjdbc, xkfssh, xprofile):
    """카프카 브로커 VM Hibernate 시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커 Hibernate 후 재개해도 메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 VM 정지 + 재시작
    vm_hibernate(xprofile, 'kafka')
    linfo("=== wait for a while ===")
    time.sleep(5)
    vm_start(xprofile, 'kafka')

    # reboot 후 ssh 객체 재생성 필요!
    kfssh = get_kafka_ssh(xprofile)
    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(kfssh, f'{xprofile}-person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

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
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_select_proc, args=(xprofile, xcp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
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
    assert DB_ROWS + DB_PRE_ROWS == cnt


def test_ct_remote_basic(xcp_setup, xprofile, xkfssh, xjdbc):
    """원격 insert / select 로 기본적인 Change Tracking 테스트.

    - Inserter / Selector 출력은 count 가 끝난 뒤 몰아서 나옴.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_select_proc, args=(xprofile, xcp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_insert_proc, args=(xprofile, xcp_setup, pid))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_cdc_local_basic(xdbzm, xkfssh, xsetup, xprofile):
    """로컬 insert / select 로 기본적인 Change Data Capture 테스트.

    - 테스트 시작전 이전 토픽을 참고하는 것이 없어야 함. (delete_topic 에러 발생)

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'db1.test.person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_cdc_remote_basic(xcp_setup, xdbzm, xprofile, xkfssh, xcdc):
    """원격 insert / select 로 기본적인 Change Data Capture 테스트.

    - 테스트 시작전 이전 토픽을 참고하는 것이 없어야 함. (delete_topic 에러 발생)

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_select_proc, args=(xprofile, xcp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=remote_insert_proc, args=(xprofile, xcp_setup, pid))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'db1.test.person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")
