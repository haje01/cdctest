import time
from multiprocessing import Process

import pytest

from kfktest.util import (SSH, count_topic_message, get_kafka_ssh, ssh_exec, local_exec,
    start_kafka_broker, stop_kafka_broker, kill_proc_by_port, vm_stop, vm_start,
    # 픽스쳐들
    xsetup, xcp_setup, xsocon, xtable, xtopic, xkafka, xzookeeper, xkvmstart,
    xconn
)

NUM_INSEL_PROCS = 5

@pytest.fixture(scope="session")
def xprofile():
    return 'mssql'


def _local_select_proc(pid):
    """로컬에서 가짜 데이터 셀렉트."""
    print(f"Select process {pid} start")
    cmd = f"cd ../deploy/mssql && python -m kfktest.selector mssql -p {pid} -d"
    local_exec(cmd)
    print(f"Select process {pid} done")


def _local_insert_proc(pid, epoch, batch):
    """로컬에서 가짜 데이터 인서트."""
    print(f"Insert process start: {pid}")
    cmd = f"cd ../deploy/mssql && python -m kfktest.inserter mssql -p {pid} -e {epoch} -b {batch} -d"
    local_exec(cmd)
    print(f"Insert process done: {pid}")


def test_ct_local_basic(xsetup, xsocon, xprofile):
    """로컬 insert / select 로 기본적인 Change Tracking 테스트."""
    cons_ip = xsetup['consumer_public_ip']['value']
    kafka_ip = xsetup['kafka_private_ip']['value']
    ssh = SSH(cons_ip, 'consumer')

    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_select_proc, args=(pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_insert_proc, args=(pid, 100, 100))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")


def test_ct_broker_stop(xsetup, xsocon, xprofile):
    """카프카 브로커 정상 정지시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커를 멈추고 (Stop Gracefully) 잠시 후 다시 기동해도
        메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_select_proc, args=(pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_insert_proc, args=(pid, 100, 100))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 stop
    time.sleep(7)
    stop_kafka_broker(xprofile)
    # 잠시 후 카프카 브로커 start
    time.sleep(10)
    start_kafka_broker(xprofile)

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")


def test_ct_broker_down(xsetup, xsocon, xprofile):
    """카프카 브로커 다운시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커 프로세스를 강제로 죽인 후, 잠시 후 다시 재개해도
        메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_select_proc, args=(pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_insert_proc, args=(pid, 100, 100))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 강제 종료
    time.sleep(7)
    kill_proc_by_port(get_kafka_ssh(xprofile), 9092)
    # 잠시 후 카프카 브로커 start (한 번 재시도 필요!?)
    time.sleep(10)
    start_kafka_broker(xprofile)

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=20)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")


def test_ct_broker_vmstop(xsetup, xsocon, xprofile):
    """카프카 브로커 VM 정지시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커가 정지 후 재개해도 메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_select_proc, args=(pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_insert_proc, args=(pid, 100, 100))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 VM 정지
    time.sleep(7)
    vm_stop(xprofile, 'kafka')
    # 잠시 후 카프카 브로커 VM 재개
    time.sleep(10)
    vm_start(xprofile, 'kafka')

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")


def test_ct_broker_vmstop(xsetup, xsocon, xprofile):
    """카프카 브로커 VM 정지시 Change Tracking 테스트.

    Insert / Select 시작 후 브로커가 정지 후 재개해도 메시지 수가 일치.

    """
    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_select_proc, args=(pid,))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_local_insert_proc, args=(pid, 1, 100))
        ins_pros.append(p)
        p.start()

    # 잠시 후 카프카 브로커 VM 정지후 재시작
    time.sleep(2)
    vm_stop(xprofile, 'kafka')
    # VM 정지 후 재시작하면, 모든 서비스는 내려간 상태
    # -> 명시적으로 서비스를 띄워 주어야 한다.
    vm_start(xprofile, 'kafka')
    import pdb; pdb.set_trace()

    # 카프카 토픽 확인 (timeout 되기 전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")


def _remote_select_proc(setup, pid):
    """원격 셀렉트 노드에서 가짜 데이터 셀렉트 (원격 노드에 setup.json 있어야 함)."""
    print(f"Select process start: {pid}")
    sel_ip = setup['selector_public_ip']['value']
    ssh = SSH(sel_ip, 'selector')
    cmd = f"cd kfktest/deploy/mssql && python3 -m kfktest.selector mssql -p {pid}"
    ret = ssh_exec(ssh, cmd, False)
    print(ret)
    print(f"Select process done: {pid}")
    return ret


def _remote_insert_proc(setup, pid, epoch, batch):
    """원격 인서트 노드에서 가짜 데이터 인서트 (원격 노드에 setup.json 있어야 함)."""
    print(f"Insert process start: {pid}")
    ins_ip = setup['inserter_public_ip']['value']
    ssh = SSH(ins_ip, 'inserter')
    cmd = f"cd kfktest/deploy/mssql && python3 -m kfktest.inserter mssql -p {pid} -e {epoch} -b {batch}"
    ret = ssh_exec(ssh, cmd, False)
    print(ret)
    print(f"Insert process done: {pid}")
    return ret


def test_ct_remote_basic(xcp_setup, xsocon, xprofile):
    """원격 insert / select 로 기본적인 Change Tracking 테스트."""
    cons_ip = xcp_setup['consumer_public_ip']['value']
    kafka_ip = xcp_setup['kafka_private_ip']['value']
    ssh = SSH(cons_ip, 'consumer')

    # Selector 프로세스들 시작
    sel_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_remote_select_proc, args=(xcp_setup, pid))
        sel_pros.append(p)
        p.start()

    # Insert 프로세스들 시작
    ins_pros = []
    for pid in range(1, NUM_INSEL_PROCS + 1):
        # insert 프로세스
        p = Process(target=_remote_insert_proc, args=(xcp_setup, pid, 100, 100))
        ins_pros.append(p)
        p.start()

    # 카프카 토픽 확인 (timeout 되기전에 다 받아야 함)
    cnt = count_topic_message(xprofile, f'{xprofile}-person', timeout=10)
    assert 10000 * NUM_INSEL_PROCS == cnt

    for p in ins_pros:
        p.join()
    print("All insert processes are done.")

    for p in sel_pros:
        p.join()
    print("All select processes are done.")
