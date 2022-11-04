from re import L
import time
import json
from multiprocessing import Process

import pytest
from confluent_kafka import Producer, Consumer

from kfktest.util import (delete_schema, get_ksqldb_ssh, local_produce_proc,
    linfo, remote_produce_proc, count_topic_message, s3_count_sinkmsg,
    KFKTEST_S3_BUCKET, KFKTEST_S3_DIR, unregister_kconn, register_s3sink,
    load_setup, _hash, kill_proc_by_port, start_kafka_broker, ssh_exec,
    ksql_exec, list_ksql_tables, list_ksql_streams, delete_ksql_objects,
    _ksql_exec, setup_filebeat, producer_logger_proc, SSH, create_topic,
    register_schema, delete_schema, consume_iter, new_consumer,
    # 픽스쳐들
    xsetup, xtopic, xkfssh, xkvmstart, xcp_setup, xs3sink, xhash, xs3rmdir,
    xrmcons, xconn, xkafka, xzookeeper, xksql, xlog, xksqlssh
)
from kfktest.producer import produce
from kfktest.consumer import consume

NUM_PRO_PROCS = 4
PROC_NUM_MSG = 10000


@pytest.fixture(scope="session")
def xprofile():
    return 'nodb'


def test_local_basic(xkafka, xprofile, xsetup, xtopic, xkfssh):
    """로컬 프로듀서 및 컨슈머로 기본 동작 테스트."""
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, PROC_NUM_MSG, 1))
        p.start()
        pro_pros.append(p)

    for p in pro_pros:
        p.join()

    # 메시지 수집 대기
    time.sleep(3)
    cnt = count_topic_message(xprofile, xtopic)
    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. {int(vel)} rows per seconds.")
    assert tot_msg == cnt


def test_local_basic_brk(xkafka, xprofile, xsetup, xtopic, xkfssh):
    """브로커가 죽을 때 로컬 프로듀서 및 컨슈머로 동작 테스트.

    - 브로커가 죽으면 메시지 손실이 발생할 수 있음 (아닌 경우도 있음)

    """
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, PROC_NUM_MSG))
        p.start()
        pro_pros.append(p)

    # 잠시 후 카프카 브로커 강제 종료
    time.sleep(2)
    kill_proc_by_port(xkfssh, 9092)
    # 잠시 후 카프카 브로커 start
    time.sleep(2)
    start_kafka_broker(xkfssh)

    for p in pro_pros:
        p.join()

    # 메시지 수집 대기
    time.sleep(5)
    cnt = count_topic_message(xprofile, xtopic)
    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. topic has {cnt} messages.")
    assert tot_msg >= cnt


def test_remote_basic(xkafka, xprofile, xsetup, xcp_setup, xtopic, xkfssh):
    """원격 프로듀서 및 컨슈머로 기본 동작 테스트."""
    st = time.time()
    # Producer 프로세스 시작
    pro_pros = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=remote_produce_proc, args=(xprofile, xsetup, pid, PROC_NUM_MSG))
        p.start()
        pro_pros.append(p)

    for p in pro_pros:
        p.join()

    time.sleep(3)
    cnt = count_topic_message(xprofile, xtopic)
    tot_msg = PROC_NUM_MSG * NUM_PRO_PROCS
    vel = tot_msg / (time.time() - st)
    linfo (f"Produce and consume total {tot_msg} messages. {int(vel)} rows per seconds.")
    assert tot_msg == cnt


@pytest.mark.parametrize('xtopic', [{
    'partitions': 1,
    'topic_cfg': {
        # 5초 마다 로그 컴팩션 하도록 토픽 생성
        'cleanup.policy': 'compact',
        # 1초 마다 세그먼트가 닫힘
        'segment.ms': 1000,
        # dirty ratio: 헤드 세그먼트의 바이트 / 전체 (헤드 + 테일) 세그먼트의 바이트가 이값보다 크면 클리닝(컴팩션) 시작
        'min.cleanable.dirty.ratio': 0.001  # 항상 수행되게
        }
     }], indirect=True)
def test_log_comp(xprofile, xcp_setup, xtopic):
    """로그 컴팩션 테스트.

    - 로그 컴팩션은 카프카의 효율을 위한 것이지, 중복 메시지 제거를 위한 것이 아님
      https://stackoverflow.com/questions/61552299/is-kafka-log-compaction-also-a-de-duplication-mechanism
    - 같은 키의 메시지가 여러 세그먼트에 존재하면 중복 메시지 발생 가능

    """
    setup = load_setup(xprofile)
    broker = f"{setup['kafka_public_ip']['value']}:19092"
    # 메시지 생성
    prod = Producer({
        'bootstrap.servers': broker
        })
    prod.produce(xtopic, b'Bob', b'100')
    prod.produce(xtopic, b'Lucy', b'100')
    prod.flush()
    time.sleep(1)  # 세그먼트 종료 대기

    prod.produce(xtopic, b'Bob', b'200')
    prod.produce(xtopic, b'Lucy', b'200')
    prod.produce(xtopic, b'Patric', b'200')
    prod.flush()
    time.sleep(1)  # 세그먼트 종료 대기

    # 로그 컴팩션은 헤드 세그먼트가 존재할 때 종료된 세그먼트들에 대해 수행되고
    # 컴팩션 결과 종료된 세그먼트들은 지워지고 하나의 테일 세그만트만 남는다.

    prod.produce(xtopic, b'Patric', b'300')
    prod.flush()

    decoder = lambda x: x.decode('utf-8')

    # 로그 컴팩션 완료를 기다린 후 결과 확인
    time.sleep(13)
    cons = Consumer(
        {
            'bootstrap.servers': broker,
            'auto.offset.reset': 'earliest',
            'group.id': 'foo'
        }
        # bootstrap_servers=broker,
        # auto_offset_reset='earliest',
        # consumer_timeout_ms=5000,
        # key_deserializer=decoder,
        # value_deserializer=decoder
    )

    for msg in consume_iter(new_consumer(), [xtopic]):
        key, value = msg.key(), msg.value()
        print(key, value)
        if key in ('Bob', 'Lucy'):
            # 테일 세그먼트에는 중복이 없음
            assert value == '200'
        if key == 'Patric':
            # 헤드 세그먼트의 메시지 중복
            assert value in ('200', '300')


S3SK_NUM_MSG = 1000
@pytest.mark.parametrize('xs3sink', [{'flush_size': S3SK_NUM_MSG // 3}], indirect=True)
def test_s3sink(xprofile, xcp_setup, xtopic, xkfssh, xs3sink):
    """토픽에 올린 데이터가 S3 로 잘 Sink 되는지"""
    # 토픽에 가짜 데이터 생성
    procs = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, S3SK_NUM_MSG))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    # rotate.schedule.interval.ms 가 지나도록 대기
    time.sleep(30)

    tot_msg = NUM_PRO_PROCS * S3SK_NUM_MSG
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, S3 Messages: {s3cnt}")
    assert tot_msg == s3cnt


s3rr_hash = _hash()
s3rr_param = {
    # 전체 메시지 수는 NUM_MSG * NUM_PRO_PROCS 이고, 이것을 파티션별로 나눠가진다.
    # 하나 이상의 파일이 생기도록 적당히 flush
    'flush_size': S3SK_NUM_MSG // 3,
    'chash': s3rr_hash
}
@pytest.mark.parametrize('xs3sink', [s3rr_param], indirect=True)
def test_s3sink_rereg(xprofile, xcp_setup, xtopic, xkfssh, xs3sink):
    """S3 Sink 커넥터를 재등록 테스트.

    - S3 Sink 는 전체 파이프라인에서 병목이 될 가능성 높음
    - 최적화를 위해 잦은 패러미터 튜닝이 필요할 수 있음
    - 이를 위해서는 커넥터 delete 후 재등록 해야하는데.. 안전할까?
     => 커넥터 이름만 같게 해주면 일반적으로 문제 없음 (메시지 중복은 발생 가능)

    """
    # 토픽에 가짜 데이터 생성
    # Producer 프로세스 시작
    procs = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, S3SK_NUM_MSG))
        p.start()
        procs.append(p)

    time.sleep(3)
    # 잠시 후 S3 Sink 재등록 (설정 바꾸는 상황 가정)
    cname = f"s3sink-nodb-{s3rr_hash}"
    unregister_kconn(xkfssh, cname)
    time.sleep(3)
    register_s3sink(xkfssh, xprofile, 'nodb_person', s3rr_param)

    for p in procs:
        p.join()

    # rotate.schedule.interval.ms 가 지나도록 대기
    time.sleep(10)

    tot_msg = NUM_PRO_PROCS * S3SK_NUM_MSG
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, S3 Messages: {s3cnt}")
    assert tot_msg == s3cnt


# S3SBK_NUM_MSG = 1000
S3SBK_NUM_MSG = 4000
@pytest.mark.parametrize('xs3sink', [{'flush_size': S3SBK_NUM_MSG // 3}], indirect=True)
def test_s3sink_brk(xkafka, xprofile, xcp_setup, xtopic, xkfssh, xs3sink):
    """브로커가 죽은 후 떠도 S3 Sink 가 잘 되는지.

    프로듀서가 메시지를 모두(=프로세스당 1000개) 보낸 후 브로커가 죽는 경우
    - 지연이 있으나 최종적으로는 S3 에 모든 메시지가 올라감

    프로듀서가 메시지를 모두 보내지 못하고(=프로세스당 총 4000개 중 1000개) 브로커가 죽는 경우
    - 토픽 레벨에서 메시지 손실 발생
    - 토픽에 있는 메시지는 모두 S3 에 올라감

    """
    # 토픽에 가짜 데이터 생성
    procs = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, S3SBK_NUM_MSG))
        p.start()
        procs.append(p)

    # 잠시 후 카프카 브로커 강제 종료
    time.sleep(1)
    kill_proc_by_port(xkfssh, 9092)
    # 잠시 후 카프카 브로커 start
    time.sleep(1)
    start_kafka_broker(xkfssh)

    for p in procs:
        p.join()

    # 이 경우는 rotate.schedule.interval.ms 가 지난 후 더 기다려야함
    # 상황에 따라 속도 차이가 있기에 여유를 많이 둠
    time.sleep(40)

    tot_msg = NUM_PRO_PROCS * S3SBK_NUM_MSG
    tocnt = count_topic_message(xprofile, xtopic)
    # S3 Sink 커넥터가 올린 내용 확인
    s3cnt = s3_count_sinkmsg(KFKTEST_S3_BUCKET, KFKTEST_S3_DIR + "/")
    linfo(f"Orignal Messages: {tot_msg}, Topic Messages: {tocnt}, S3 Messages: {s3cnt}")
    assert tocnt == s3cnt

##
#  TODO: S3 Sink Field Partitioner 테스트
#    db.timezone 설정 필요?
#
#

@pytest.fixture
def xdel_ksql_basic_strtbl(xprofile):
    """의존성을 고려한 테이블 및 스트림 삭제."""
    ssh = get_ksqldb_ssh(xprofile)
    delete_ksql_objects(ssh, [
        (1, 'person_tbl'), (0, 'person'),
        ])


def test_ksql_basic(xkafka, xprofile, xcp_setup, xtopic, xksql, xdel_ksql_basic_strtbl):
    """ksqlDB 기본 동작 테스트."""
    ksql_exec(xprofile, 'show streams')

    # 토픽에 가짜 데이터 생성
    procs = []
    for pid in range(1, NUM_PRO_PROCS + 1):
        p = Process(target=local_produce_proc, args=(xprofile, pid, 10, 1, 0, 0,
                True))  # 메시지 키 이용
        p.start()

    for p in procs:
        p.join()

    # 토픽에서 스트림 생성
    sql = '''
    CREATE STREAM person (
        pidid VARCHAR KEY,
        id VARCHAR, name VARCHAR, address VARCHAR,
        ip VARCHAR, birth VARCHAR, company VARCHAR, phone VARCHAR)
        with (kafka_topic = 'nodb_person', partitions=12,
            value_format='json');
    '''
    ksql_exec(xprofile, sql)

    # 스트림 확인
    sql = '''
        SELECT * FROM person;
    '''
    ret = ksql_exec(xprofile, sql, 'query')
    # 헤더 제외 후 크기 확인
    time.sleep(3)
    assert len(ret[1:]) == 4 * 10

    # 스트림에서 테이블 생성
    sql = '''
    SHOW PROPERTIES;
    CREATE TABLE person_tbl AS
        SELECT pidid, COUNT(id) AS count
        FROM person WINDOW TUMBLING (SIZE 1 MINUTES)
        GROUP BY pidid;
    '''
    ret = ksql_exec(xprofile, sql)

    time.sleep(3)
    # 테이블 확인
    # 주: ksql.streams.auto.offset.reset 이 earliest 여야 함.

    sql = '''
        SELECT * FROM PERSON_TBL;
    '''
    ret = ksql_exec(xprofile, sql, 'query')
    time.sleep(3)
    # 헤더 제외 후 크기 확인
    total = 0
    for row in ret[1:]:
        cnt = row[-1]
        total += cnt
    assert total == 4 * 10


@pytest.fixture
def xdel_ksql_dedup_strtbl(xprofile, xtopic):
    """의존성을 고려한 테이블 및 스트림 삭제."""
    ssh = get_ksqldb_ssh(xprofile)
    delete_ksql_objects(ssh, [
        (0, 'person_dedup'), (0, 'person_agg_str'),
        (1, 'person_agg'), (1, 'person_tbl'), (0, 'person')
        ])

def test_ksql_dedup(xkafka, xprofile, xcp_setup, xksql, xdel_ksql_dedup_strtbl):
    """ksqlDB 로 중복 제거 테스트."""

    # 토픽에 중복이 있는 가짜 데이터 생성
    duprate=0.2
    local_produce_proc(xprofile, 1, 100, 1, duprate)

    ssh = get_ksqldb_ssh(xprofile)
    # 토픽에서 스트림 생성
    sql = '''
    CREATE STREAM person (
        id INT, name VARCHAR, address VARCHAR,
        ip VARCHAR, birth VARCHAR, company VARCHAR, phone VARCHAR)
        with (kafka_topic = 'nodb_person', partitions=12,
            value_format='json');
    '''
    _ksql_exec(ssh, sql)
    props = {
        # 버퍼링을 꺼야 바로 카운트에 반영됨
        "ksql.streams.cache.max.bytes.buffering": "0",
        # 테이블 생성시도 오프셋 리셋해 주어야 처음부터 옴 (per query)
        "ksql.streams.auto.offset.reset": "earliest",
    }

    # 윈도우별 메시지 카운팅 테이블 생성3
    # id 와 name 을 복합키로 생각
    sql = '''
    CREATE TABLE person_agg
        WITH (kafka_topic='person_agg', partitions=1, format='json')
        AS
        SELECT
            -- group by 의 기준 컬럼은 select 되어야함 (테이블의 키가 됨)
            id AS KEY1,
            name AS KEY2,
            -- 키를 값으로도 사용
            AS_VALUE(id) AS id,
            AS_VALUE(name) AS name,
            -- 같은 키 중 오프셋 기준 가장 최신 것 선택
            LATEST_BY_OFFSET(address) AS address,
            LATEST_BY_OFFSET(birth) AS birth,
            LATEST_BY_OFFSET(company) AS company,
            LATEST_BY_OFFSET(phone) AS phone,
            COUNT(*) AS count
        FROM person WINDOW TUMBLING (SIZE 1 MINUTES)
        GROUP BY id, name;

    -- 메시지 카운팅 스트림
    CREATE STREAM person_agg_str (
            id INT, name VARCHAR, address VARCHAR, ip VARCHAR,
            birth VARCHAR, company VARCHAR, phone VARCHAR, count int)
        WITH (kafka_topic = 'person_agg', partitions=1, format='json');

    -- 중복 제거된 스트림
    CREATE STREAM person_dedup AS
        SELECT
            id, name, address, ip, birth, company, phone
        FROM person_agg_str
        WHERE count = 1
        PARTITION BY id
    '''
    _ksql_exec(ssh, sql, 'ksql', props)

    # 중복 제거 확인
    cnt = count_topic_message(xprofile, 'PERSON_DEDUP')
    assert cnt == 100


## TODO
#
# 기존 토픽의 파티션 수 바꾸는 테스트
#

def test_filebeat(xkafka, xprofile, xtopic, xcp_setup, xlog):
    """프로듀서 Filebeat 테스트.

    생성된 로그 파일의 메시지 수와 파일비트를 통해 Kafka 로 전송된 메시지 수가 같아야 함.

    """
    # 프로듀서에 파일비트 설정 후 재시작
    setup_filebeat(xprofile)

    # 프로듀서에서 logger 파일 생성
    producer_logger_proc(xprofile, messages=10000, latency=0)

    time.sleep(5)
    cnt = count_topic_message(xprofile, xtopic)
    assert 10000 == cnt


@pytest.fixture
def xdel_ksql_flatjson(xprofile, xtopic):
    """의존성을 고려한 테이블 및 스트림 삭제."""
    ssh = get_ksqldb_ssh(xprofile)
    delete_ksql_objects(ssh, [
        (0, 'person_flat2'), (0, 'person_flat1'), (0, 'person_merge'), (0, 'person_raw'),
        ])

@pytest.mark.parametrize('xs3sink', [{'topics': "PERSON_FLAT2"}], indirect=True)
def test_ksql_flatjson(xkafka, xprofile, xtopic, xksql, xdel_ksql_flatjson, xs3sink):
    """ksqlDB 를 사용해 Nested JSON 펼치기.

    - 임의의 깊이까지 완전히 펼치는 것은 어렵기에, 정해진 깊이까지만 펼침
        - 그 깊이의 노드까지는 필드까지 쿼리 가능
        - 그 깊이 이상의 노드는 추가 파싱 필요
    - 펼쳐진 메시지가 S3에 올라가는 것 확인

    원본 로그 구조:
        2022-10-05 09:20:35,688
        {
            "Header": {
                "TranId": "00000000-0000-0000-0000-000000000000",
                "Actor": {
                    "WorldId": 10,
                    "UserId": 28,
                    "CharId": 1984075,
                    "CharName": "hsm0000",
                    "CharLevel": 103,
                    "CharClass": "NoviceHunter"
                }
            },
            "Body": {
                "Action": "CharLogin",
                "Infos": [{
                    "Domain": "LoginOut",
                    "LoginTime": "2022-10-05T00:20:35.3757353Z"
                }]
            }
        }

    펼쳐진 로그 구조:

    {
        "REGDATE": "2022-10-05 11:21:52,578",
        "TRANID": "882a1bb7-c553-4e77-8f6b-69081b9ebc7b"
        "ACTOR": {
            "UserId": "177724",
            "CharLevel": "100",
            "CharClass": "WhiteWizard",
            "CharName": "jhbnmjui",
            "WorldId": "10",
            "CharId": "2083227"
        },
        "ACTION": "ShopBuy",
        "INFO0: {
            "DeltaType": "Consume",
            "MoneyType": "Gem",
            "MoneyAmount": "957350",
            "DeltaAmount": "2000",
            "Domain": "Money"
        },
    }


    """
    setup = load_setup(xprofile)
    broker = f"{setup['kafka_public_ip']['value']}:19092"

    ssh = get_ksqldb_ssh(xprofile)

    # nested json 파일 토픽에 보내기
    prod = Producer({'bootstrap.servers': broker})
    with open('../refers/nested_json.txt') as f:
        for line in f:
            prod.produce('nodb_person', value=line.encode())

    # 그대로의 스트림 생성
    sql = '''
        CREATE STREAM person_raw (str VARCHAR) WITH (kafka_topic='nodb_person', partitions=12, value_format='kafka')
    '''
    props = {
        # 처음부터 카운트 (per query)
        "ksql.streams.auto.offset.reset": "earliest",
    }
    ret = _ksql_exec(ssh, sql, 'ksql', props)
    assert ret[0]['commandStatus']['status'] == 'SUCCESS'

    # 일시와 JSON 결합 스트림 생성
    sql = '''
        CREATE STREAM person_merge WITH (kafka_topic='person_raw', partitions=1, format='json') AS SELECT JSON_CONCAT('{"RegDate": "'+TRIM(SUBSTRING(str, 1, 24))+'"}', SUBSTRING(str, 25)) msg FROM PERSON_RAW;
    '''
    ret = _ksql_exec(ssh, sql, 'ksql', props)
    assert ret[0]['commandStatus']['status'] == 'SUCCESS'

    # Flat 스트림 구성
    sql = '''
        -- 1 단계 플랫: TranId, Actor, Body 레벨 플랫화
        CREATE STREAM person_flat1 AS
            SELECT
                EXTRACTJSONFIELD(MSG, '$.RegDate') RegDate,
                EXTRACTJSONFIELD(MSG, '$.Header.TranId') TranId,
                JSON_RECORDS(EXTRACTJSONFIELD(MSG, '$.Header.Actor')) Actor,
                JSON_RECORDS(EXTRACTJSONFIELD(MSG, '$.Body')) Body
            FROM person_merge;

        -- 2 단계 플랫:Body 아래 Action / Infos 레벨 플랫화
        CREATE STREAM person_flat2 AS
            SELECT
                RegDate, TranId, Actor,
                Body['Action'] Action,
                JSON_RECORDS(EXTRACTJSONFIELD(Body['Infos'], '$[0]')) Info0
            FROM person_flat1;
    '''

    ret = _ksql_exec(ssh, sql, 'ksql', props)
    assert ret[0]['commandStatus']['status'] == 'SUCCESS'

    sql = '''
        SELECT Info0['Domain'] FROM person_flat2 LIMIT 1;
    '''
    ret = _ksql_exec(ssh, sql, 'query')
    assert len(ret[1][0]) > 0


@pytest.fixture
def xdel_ksql_joinss(xprofile):
    """의존성을 고려한 테이블 및 스트림 삭제."""
    ssh = get_ksqldb_ssh(xprofile)
    delete_ksql_objects(ssh, [
        (1, 'latest_email'), (0, 'person_email'), (1, 'person_agg'), (0, 'person'),
        (0, 'email'),
        ])

def test_ksql_joinss(xkafka, xprofile, xtopic, xksql, xkfssh, xdel_ksql_joinss):
    """이벤트와 가까운 시간대의 정보를 스트림+스트림 이너 조인

    - person 스트림에 더해 별도 정보 스트림 생성
    - 정보 스트림은 나중에 새로운 레코드 들어옴
        - id 값이 2의 배수는 과거 정보, 3의 배수는 새 정보 있음
    - 이벤트 레코드의 시간과 가까운 정보 레코드를 조인
        - 이벤트 레코드 시간과 관계없이 최신 정보 조인이 필요하면 스트림+테이블 조인

    - 시간대 (5초) 를 WITHIN 으로 명시하고 INNER 조인
        - 시간대를 벗어나는 정보를 갖는 레코드는 제외되는 결과

    참고:
        https://docs.ksqldb.io/en/latest/developer-guide/joins/join-streams-and-tables/
        https://developer.confluent.io/tutorials/join-a-stream-to-a-stream/ksql.html
        https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/

    """
    # person 토픽 & 스트림 생성
    local_produce_proc(xprofile, 1, 10, with_key=True, with_ts=True)

    sql = '''
    CREATE STREAM person (
        key VARCHAR KEY,
        id INT, regts TIMESTAMP, name VARCHAR, address VARCHAR,
        ip VARCHAR, birth VARCHAR, company VARCHAR, phone VARCHAR)
        with (kafka_topic = 'nodb_person', value_format='json', timestamp='regts');
    '''
    ksql_exec(xprofile, sql)

    # 이메일 토픽 & 스트림 생성
    create_topic(xkfssh, 'email', partitions=12, replications=1)

    setup = load_setup(xprofile)
    broker = f"{setup['kafka_public_ip']['value']}:19092"

    prod = Producer({
            'bootstrap.servers': broker
        })
    # 이벤트 시간대의 정보
    for i in range(1, 11):
        # id 값 2의 배수만 이벤트 시간대의 정보 있음
        if i % 2 != 0:
            continue
        email = f'old{i:02d}@email.com'
        data = {'id': i, 'regts': int(time.time() * 1000), 'email': email}
        key = f'0-{i}'.encode()
        prod.produce('email', json.dumps(data).encode(), key)
    prod.flush()

    # 새 정보와 이벤트의 시간대가 차이나도록 쉬어줌
    time.sleep(7)
    # 새 정보
    for i in range(1, 11):
        # id 값 3의 배수만 새 정보 있음
        if i % 3 != 0:
            continue
        email = f'new{i:02d}@email.com'
        data = {'id': i, 'regts': int(time.time() * 1000), 'email': email}
        key = f'0-{i}'.encode()
        prod.produce('email', json.dumps(data).encode(), key)
    prod.flush()

    sql = '''
    CREATE STREAM email (key VARCHAR KEY, id INT, regts TIMESTAMP, email VARCHAR)
        with (kafka_topic = 'email', value_format='json', timestamp='regts');
    '''
    ksql_exec(xprofile, sql)

    props = {
        # 처음부터 카운트 (per query)
        "ksql.streams.auto.offset.reset": "earliest",
    }
    sql = '''
    CREATE STREAM person_email AS
        SELECT p.id, p.name, e.email FROM person p INNER JOIN email e
            WITHIN 5 SECONDS ON p.id = e.id;
    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    # 이벤트와 같은 시간대의 old 정보만 남는 결과
    cnt = count_topic_message(xprofile, 'PERSON_EMAIL')
    assert 5 == cnt
    ret = ksql_exec(xprofile, 'SELECT * FROM person_email EMIT CHANGES',
        'query', props, timeout=7)
    for row in ret[1:]:
        assert row[2].startswith('old')

    ### 결과 (레코드 시점의 정보만 이용, 정보가 없는 행은 제거)
    # +----------------------------------+----------------------------------+
    # |P_ID                              |EMAIL                             |
    # +----------------------------------+----------------------------------+
    # |10                                |old10@email.com                   |
    # |8                                 |old08@email.com                   |
    # |4                                 |old04@email.com                   |
    # |2                                 |old02@email.com                   |
    # |6                                 |old06@email.com                   |


@pytest.fixture
def xdel_ksql_joinst(xprofile):
    """의존성을 고려한 테이블 및 스트림 삭제."""
    ssh = get_ksqldb_ssh(xprofile)
    delete_ksql_objects(ssh, [
        (0, 'person_email'), (1, 'latest_email'), (0, 'person'), (0, 'email'),
        ])

def test_ksql_joinst(xkafka, xprofile, xtopic, xksql, xkfssh, xdel_ksql_joinst):
    """이벤트와 최신 정보를 스트림+테이블 이너 조인

    - person 스트림에 더해 별도 정보 스트림 생성
    - 정보 스트림은 나중에 새로운 레코드 들어옴
        - id 값이 2의 배수는 과거 정보, 3의 배수는 새 정보 있음
    - 정보 스트림에서 최신 정보 기준으로 테이블 생성
    - 이벤트 스트림과 정보 테이블 조인

    참고:
        https://docs.ksqldb.io/en/latest/developer-guide/joins/join-streams-and-tables/
        https://developer.confluent.io/tutorials/join-a-stream-to-a-table/ksql.html
        https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/

    """
    # person 토픽 & 스트림 생성
    local_produce_proc(xprofile, 1, 10, with_key=True, with_ts=True)

    sql = '''
    CREATE STREAM person (
        key VARCHAR KEY,
        id INT, regts TIMESTAMP, name VARCHAR, address VARCHAR,
        ip VARCHAR, birth VARCHAR, company VARCHAR, phone VARCHAR)
        with (kafka_topic = 'nodb_person', value_format='json', timestamp='regts');
    '''
    ksql_exec(xprofile, sql)

    # 이메일 토픽 & 스트림 생성
    create_topic(xkfssh, 'email', partitions=12, replications=1)

    setup = load_setup(xprofile)
    broker = f"{setup['kafka_public_ip']['value']}:19092"

    prod = Producer({
        'bootstrap.servers': broker
        })
    # 과거 정보
    for i in range(1, 11):
        # 2의 배수는 과거 정보 있음
        if i % 2 != 0:
            continue
        email = f'old{i:02d}@email.com'
        data = {'id': i, 'regts': int(time.time() * 1000), 'email': email}
        key = f'0-{i}'.encode()
        prod.produce('email', json.dumps(data).encode(), key)
    prod.flush()

    # 새 정보
    for i in range(1, 11):
        # 3의 배수는 새 정보 있음
        if i % 3 != 0:
            continue
        email = f'new{i:02d}@email.com'
        data = {'id': i, 'regts': int(time.time() * 1000), 'email': email}
        key = f'0-{i}'.encode()
        prod.produce('email', json.dumps(data).encode(), key)
    prod.flush()

    # 정보 스트림 생성
    sql = '''
    CREATE STREAM email (key VARCHAR KEY, id INT, regts TIMESTAMP, email VARCHAR)
        with (kafka_topic = 'email', value_format='json', timestamp='regts');
    '''
    ksql_exec(xprofile, sql)

    # 최신 값 기준 정보 테이블 생성
    props = {
        # 처음부터 카운트 (per query)
        "ksql.streams.auto.offset.reset": "earliest",
    }
    sql = '''
    CREATE TABLE latest_email AS
        SELECT id, LATEST_BY_OFFSET(email) email FROM email GROUP BY id
    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    # 잠시 쉬어주지 않으면 결과값이 안나옴?!
    time.sleep(1)
    # 조인 스트림
    sql = '''
    CREATE STREAM person_email AS
        SELECT p.id, p.name, e.email FROM person p
            INNER JOIN latest_email e ON p.id = e.id;
    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    # 잠시 쉬어주지 않으면 결과값이 안나옴!
    time.sleep(1)
    # 갯수가 맞고 최신 정보인지 확인
    sql = '''
        SELECT * FROM person_email;
    '''
    ret = ksql_exec(xprofile, sql, 'query', props, timeout=5)
    for row in ret[1:]:
        id, name, email = row
        if id % 2 == 0 and id != 6:
            assert email.startswith('old')
        else:
            assert email.startswith('new')

    ### 결과 (최근 정보가 있는 행은 그것을, 정보가 없는 행은 제거)
    # +----------------------------------+----------------------------------+
    # |P_ID                              |EMAIL                             |
    # +----------------------------------+----------------------------------+
    # |10                                |old10@email.com                   |
    # |9                                 |new09@email.com                   |
    # |8                                 |old08@email.com                   |
    # |4                                 |old04@email.com                   |
    # |2                                 |old02@email.com                   |
    # |3                                 |new03@email.com                   |
    # |6                                 |new06@email.com                   |


# 참고: https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/

# - 구매와 가격 스트림
# - 한 구매에 대해 가격이 여러번 찍히는 경우는 마지막 것을 선택

# - 스트림-스트림 조인 : 이벤트 당시 가격 반영
#     - 새 가격이 들어오면 반영 대상 구매 범위를 어디까지 할 것인가? (WITHIN)
#         - 0: 이후 구매에 대해서만 반영
#             - 단점: 최초 동기시 가격이 늦게 들어오면 그동안 null 가격 적용
#         - 1분:
#             - 장점: 최초 동기시 가격이 늦게 들어와도 1분 이내는 null 가격 없음
#             - 단점: 최대 1분 동안 이전 가격 구매건에 최신 가격 소급 적용
#     - 장점: 일부 오차 있으나 과거 이벤트 재작업시 당시 가격 반영
#     - 스트림-스트림 조인은 코드성 테이블을 위한 것은 아닌 듯..
#        - 테이블이 벌크로 들어올 때마다 모든 로그 행이 JOIN 되어 반복 등장!

# - 스트림-테이블 조인 : 최신 가격만 반영
#     - 가격 스트림에서 Hopping 윈도우로 테이블 만듦
#         - 윈도우 크기는 가격 테이블 poll_interval 이상
#         - 최종 가격으로 테이블 만듦
#     - 단점:
#         - 과거 이벤트 재작업시 모든 구매에 최신 가격 적용됨
#         - 비대칭적이기에 가격이 늦게 들어온 구매는 null 가격 적용
#     - 장점:

@pytest.fixture
def xdel_ksql_fillna(xprofile):
    """의존성을 고려한 테이블 및 스트림 삭제."""
    ssh = get_ksqldb_ssh(xprofile)
    delete_ksql_objects(ssh, [
        (0, 'person_email_fill'), (0, 'person_email'), (1, 'latest_email'), (1, 'earliest_email'),
        (0, 'person'), (0, 'person_raw'), (0, 'email'),
        ])
def test_ksql_fillna(xkafka, xprofile, xtopic, xksql, xkfssh, xdel_ksql_fillna):
    """로그 스트림과 코드성 테이블 레프트 조인시 시점 문제로 null 값이 나오는 경우와 대응.

    - 처리 순서는 레코드의 타임스탬프 (보통 rowtime) 순
        - 테이블이나 스트림 CREATE 시간과는 무관
    - 따라서, 테이블 레코드가 스트림 레코드보다 늦게 들어왔으면
        레프트 조인 결과는 null 값이 됨
    - 때로는 테이블 레코드가 먼저 왔어도 null 인 경우도 있음.. -_-;
        - 내부에서 전파되는 데 시간이 필요할 것으로 추정
        - 전파되기 전에 사용하면 null 이 되는 듯
        - 실재 인터랙션이 진행되는 정도의 시간차를 두고 실행하면 문제 없는 듯
    - null 값을 같은 키의 가장 가까운 값으로 fillna

    부언:
        - 코파티셔닝 동작 확인을 위해 partitions=4 이용
            - JOIN ON 에 들어가는 키도 맞춰주어야
        - 조인 테이블을 만들 때는 참조하는 스트림 및 테이블이 생성된 후
          1초 정도 시간이 지난 후에 하지 않으면 결과가 나오지 않는다!

    참고:
        https://groups.google.com/g/ksql-users/c/7YUMT8OY4Gg

    """
    # 필요한 스트림 & 테이블 먼저 선언
    props = {
        # 처음부터 카운트 (per query)
        "ksql.streams.auto.offset.reset": "earliest",
    }

    sql = '''
    -- person 스트림
    CREATE STREAM person (
            id INT KEY,   -- 키가 VARCHAR 이외 타입인 경우 json 포맷 사용해야
            name VARCHAR)
        WITH (kafka_topic='person', format='json', partitions=4);

    -- 이메일 스트림
    CREATE STREAM email (
            id INT KEY,   -- 키가 VARCHAR 이외 타입인 경우 json 포맷 사용해야
            email VARCHAR)
        WITH (kafka_topic='email', format='json', partitions=4);

    -- 최신 이메일 스트림
    CREATE TABLE latest_email AS
        SELECT
            id,
            LATEST_BY_OFFSET(email) email
        FROM email GROUP BY id;

    -- 최초 이메일 스트림
    CREATE TABLE earliest_email
        WITH (kafka_topic='earliest_email', timestamp='regdt')
        AS SELECT
            CAST(0 AS BIGINT) regdt,
            id,
            EARLIEST_BY_OFFSET(email) email
        FROM email GROUP BY id;

    -- 조인 결과 스트림
    CREATE STREAM person_email AS
        SELECT p.id id, name, email FROM person p
            LEFT JOIN latest_email e ON p.id = e.id
            EMIT CHANGES;
    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    # 스트림/테이블/토픽 생성시에 시간이 걸림
    # 바로 insert 하면 유실
    time.sleep(6)
    # 로그보다 먼저 들어가 있던 코드
    sql = f'''
    INSERT INTO email (id, email) VALUES(1, '1-1@email.com');
    INSERT INTO email (id, email) VALUES(2, '2-1@email.com');
    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    time.sleep(1)
    # 로그 인서트
    sql = f'''
    INSERT INTO person (id, name) VALUES(1, 'Brittany Kelley 1');
    INSERT INTO person (id, name) VALUES(2, 'Jeremy Allen 1');
    INSERT INTO person (id, name) VALUES(3, 'Daniel Banks 1');
    INSERT INTO person (id, name) VALUES(4, 'Charles Rice 1');    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    time.sleep(1)
    # 로그 이후 들어온 코드
    sql = f'''
    INSERT INTO email (rowtime, id, email) VALUES(0, 3, '3-1@email.com');
    INSERT INTO email (rowtime, id, email) VALUES(0, 4, '4-1@email.com');
    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    time.sleep(3)
    sql = '''
    SELECT * FROM person_email
    '''
    ret = ksql_exec(xprofile, sql, 'query', props)
    # 코드가 로그보다 나중에 들어온 경우는 NULL
    for row in ret[1:]:
        pid = row[0]
        if pid in (1, 2):
            assert row[2] is not None
        elif pid in (3, 4):
            assert row[2] is None

    # 아래와 같은 결과
    # +--------------------------------+--------------------------------+--------------------------------+
    # |P_ID                            |NAME                            |EMAIL                           |
    # +--------------------------------+--------------------------------+--------------------------------+
    # |1                               |Brittany Kelley                 |1-1@email.com                   |
    # |2                               |Jeremy Allen                    |2-1@email.com                   |
    # |3                               |Daniel Banks                    |null                            |
    # |4                               |Charles Rice                    |null                            |

    time.sleep(1)
    # 코드 변경
    sql = f'''
    INSERT INTO email (id, email) VALUES(1, '1-2@email.com');
    INSERT INTO email (id, email) VALUES(2, '2-2@email.com');
    INSERT INTO email (id, email) VALUES(3, '3-2@email.com');
    INSERT INTO email (id, email) VALUES(4, '4-2@email.com');
    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    time.sleep(3)
    # 로그 두 번째 인서트
    sql = f'''
    INSERT INTO person (id, name) VALUES(1, 'Brittany Kelley 2');
    INSERT INTO person (id, name) VALUES(2, 'Jeremy Allen 2');
    INSERT INTO person (id, name) VALUES(3, 'Daniel Banks 2');
    INSERT INTO person (id, name) VALUES(4, 'Charles Rice 2');    '''
    ksql_exec(xprofile, sql, 'ksql', props)

    time.sleep(1)
    sql = '''
    SELECT * FROM person_email
    '''
    ret = ksql_exec(xprofile, sql, 'query', props)
    # 두 번째 인서트된 로그는 두 번재 코드와 조인되어야 함
    for row in ret[1:]:
        pid = row[0]
        second = row[1].endswith('2')
        email = row[2]
        if second:
            step = email.split('@')[0].split('-')[1]
            assert step == '2'

    # 아래와 같은 결과 (순서는 보기 좋게 정리)
    # +--------------------------------+--------------------------------+--------------------------------+
    # |P_ID                            |NAME                            |EMAIL                           |
    # +--------------------------------+--------------------------------+--------------------------------+
    # |1                               |Brittany Kelley                 |1-1@email.com                   |
    # |2                               |Jeremy Allen                    |2-1@email.com                   |
    # |3                               |Daniel Banks                    |null                            |
    # |4                               |Charles Rice                    |null                            |
    # |1                               |Brittany Kelley 2               |1-2@email.com                   |
    # |2                               |Jeremy Allen 2                  |2-2@email.com                   |
    # |3                               |Daniel Banks 2                  |3-2@email.com                   |
    # |4                               |Charles Rice 2                  |4-2@email.com                   |

    sql = '''
    -- 조인 결과에서 null 값을 최초 코드로 채워준 스트림
    CREATE STREAM person_email_fill AS
        SELECT p.id, name, IFNULL(p.email, e.email) FROM person_email p
            LEFT JOIN earliest_email e ON p.id = e.id
            EMIT CHANGES;
    '''
    ret = ksql_exec(xprofile, sql, 'ksql', props)

    time.sleep(5)
    sql = '''
    SELECT * FROM person_email_fill
    '''
    ret = ksql_exec(xprofile, sql, 'query', props)

    # 모든 email 이 채워져 있음
    for row in ret[1:]:
        pid = row[0]
        second = row[1].endswith('2')
        email = row[2]
        assert email is not None
        step = email.split('@')[0].split('-')[1]
        # null 값은 처음 값으로 채워져 있어야
        if pid in (3, 4) and not second:
            assert step == '1'
        # 갱신된 코드 확인
        if second:
            assert step == '2'

    # 아래와 같은 결과 (순서는 보기 좋게 정리)
    # +--------------------------------+--------------------------------+--------------------------------+
    # |P_ID                            |NAME                            |KSQL_COL_0                      |
    # +--------------------------------+--------------------------------+--------------------------------+
    # |1                               |Brittany Kelley                 |1-1@email.com                   |
    # |2                               |Jeremy Allen                    |2-1@email.com                   |
    # |3                               |Daniel Banks                    |3-1@email.com      (채워짐)     |
    # |4                               |Charles Rice                    |4-1@email.com      (채워짐)     |
    # |1                               |Brittany Kelley 2               |1-2@email.com                   |
    # |2                               |Jeremy Allen 2                  |2-2@email.com                   |
    # |4                               |Charles Rice 2                  |4-2@email.com                   |
    # |3                               |Daniel Banks 2                  |3-2@email.com                   |


def test_ksql_offset():
    """auto.offset.reset 의 특성 테스트.

    - 스트림/테이블 생성시 설정된 offset reset 은 select 그대로 적용된다.
    - 그러나, 그 스트림/테이블이 JOIN 되어 다른 스트림/테이블 에사용되면 그것의 offset reset 설정을 따른다.

    """



def test_schema_register(xkafka, xprofile, xtopic, xksqlssh):
    """스키마 등록 테스트.

    - SR 의 기본 호환성 설정은 BACKWARD
        - 새 스키마를 이용하는 컨슈머가 옛 프로듀서 데이터를 읽을 수 있음
    - 이를 위해 새로 추가되는 필드에는 기본값이 있어야 함
    - FORWARD 호환성: 옛 스키마를 이용하는 컨슈머가 새 프로듀서 데이터를 읽을 수 있음


    """
    delete_schema(xksqlssh, 'person')

    # 최초 스키마 등록
    schema = {
        "type": "record",
        "name": "person",
        "namespace": "io.kfktest",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "regdt", "type": "string"},
            {"name": "pid", "type": "int", "default": 0},
            {"name": "sid", "type": "int", "default": 0},
        ]
    }
    ret = register_schema(xksqlssh, schema)
    id1 = ret['id']

    # 같은 스키마를 반복 등록하면 같은 id 반환
    schema = {
        "type": "record",
        "name": "person",
        "namespace": "io.kfktest",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "regdt", "type": "string"},
            {"name": "pid", "type": "int", "default": 0},
            {"name": "sid", "type": "int", "default": 0},
        ]
    }
    ret = register_schema(xksqlssh, schema)
    assert ret['id'] == id1

    # 추가 필드
    schema = {
        "type": "record",
        "name": "person",
        "namespace": "io.kfktest",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "regdt", "type": "string"},
            {"name": "pid", "type": "int", "default": 0},
            {"name": "sid", "type": "int", "default": 0},
            {"name": "name", "type": "string"},
            {"name": "address", "type": "string"},
            {"name": "ip", "type": "string"},
        ]
    }
    ret = register_schema(xksqlssh, schema, False)
    # 추가 필드 기본값 없음 -> 호환성이 없어 스키마 등록 실패
    assert 'READER_FIELD_MISSING_DEFAULT_VALUE' in ret['message']

    # 추가 필드에 기본값을 넣어주면 등록 성공
    schema = {
        "type": "record",
        "name": "person",
        "namespace": "io.kfktest",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "regdt", "type": "string"},
            {"name": "pid", "type": "int", "default": 0},
            {"name": "sid", "type": "int", "default": 0},
            {"name": "name", "type": "string", "default": "N/A"},
            {"name": "address", "type": "string", "default": "N/A"},
            {"name": "ip", "type": "string", "default": "N/A"},
        ]
    }
    ret = register_schema(xksqlssh, schema)
    assert 'id' in ret

    # 기존 필드(기본값 있는)를 제거하고 기본값 있는 필드 추가
    schema = {
        "type": "record",
        "name": "person",
        "namespace": "io.kfktest",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "regdt", "type": "string"},
            {"name": "pid", "type": "int", "default": 0},
            {"name": "sid", "type": "int", "default": 0},
            # {"name": "name", "type": "string", "default": "N/A"},
            # {"name": "address", "type": "string", "default": "N/A"},
            # {"name": "ip", "type": "string", "default": "N/A"},
            {"name": "birth", "type": "string", "default": "N/A"},
            {"name": "company", "type": "string", "default": "N/A"},
            {"name": "phone", "type": "string", "default": "N/A"},
        ]
    }
    ret = register_schema(xksqlssh, schema)
    assert 'id' in ret