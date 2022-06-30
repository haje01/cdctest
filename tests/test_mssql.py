import time
from multiprocessing import Process

import pytest

from kfktest.util import (SSH, count_topic_message, get_kafka_ssh, ssh_exec,
    local_exec, start_kafka_broker, stop_kafka_broker, kill_proc_by_port,
    vm_stop, vm_start, restart_kafka_and_connect, stop_kafka_and_connect,
    count_table_row, DB_PRE_ROWS, local_select_proc, local_insert_proc, linfo,
    NUM_INSEL_PROCS, remote_insert_proc, remote_select_proc, DB_ROWS,
    # 픽스쳐들
    xsetup, xcp_setup, xjdbc, xtable, xtopic_ct, xkafka, xzookeeper, xkvmstart,
    xconn, xkfssh, xdbzm, xtopic_cdc, xrmcons, xcdc, xhash
    )


@pytest.fixture(scope="session")
def xprofile():
    return 'mssql'


def test_ct_local_basic(xsetup, xjdbc, xprofile, xkfssh):
    """로컬 insert / select 로 기본적인 Change Tracking 테스트."""
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

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_stop(xsetup, xjdbc, xprofile, xkfssh, xhash):
    """카프카 브로커 정상 정지시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커를 멈추고 (Stop Gracefully) 잠시 후 다시 기동해도
        메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_select_proc, args=(xprofile, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=local_insert_proc, args=(xprofile, pid))
        ins_pros.append(p)
        p.start()

    # 주: 브로커 정지 시간에 따른 메시지 중복 발생 가능
    # EPOCH 10, BATCH 10 일 때
    #   sleep 6, 7, 10 -> 중복 발생
    #   sleep 11, 13, 17 -> 중복 없음
    # EPOCH 100, BATCH 100 일 때
    #   sleep 1, 5, 7 -> 중복 없음
    time.sleep(10)
    # 중요: 의존성을 고려해 Kafka 와 Connect 정지
    stop_kafka_and_connect(xprofile, xkfssh, xhash)
    time.sleep(10)
    # 중요: 의존성을 고려해 Kafka 와 Connect 재시작
    restart_kafka_and_connect(xkfssh, xprofile, xhash, False)

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")


def test_ct_broker_down(xsetup, xjdbc, xprofile, xkfssh):
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
    time.sleep(7)
    kill_proc_by_port(xkfssh, 9092)
    # 잠시 후 카프카 브로커 start
    time.sleep(10)
    start_kafka_broker(xkfssh)

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xkfssh, f'{xprofile}-person', timeout=20)
    # 브로커만 강제 Kill 된 경우, 커넥터가 offset 을 flush 하지 못해 다시 시도
    # -> 중복 메시지 발생 가능!
    assert DB_ROWS + DB_PRE_ROWS <= cnt

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
    cnt = count_topic_message(kfssh, f'{xprofile}-person', timeout=20)
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


def test_ct_remote_basic(xcp_setup, xjdbc, xprofile, xkfssh):
    """원격 insert / select 로 기본적인 Change Tracking 테스트."""
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


def test_cdc_remote_basic(xcp_setup, xdbzm, xprofile, xkfssh, xtable):
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
    cnt = count_topic_message(xkfssh, f'db1.dbo.person', timeout=10)
    assert DB_ROWS + DB_PRE_ROWS == cnt

    for p in ins_pros:
        p.join()
    linfo("All insert processes are done.")

    for p in sel_pros:
        p.join()
    linfo("All select processes are done.")

    linfo(f"CDC Test Elapsed: {time.time() - xtable:.2f}")
