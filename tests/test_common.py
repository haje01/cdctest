import pdb

from requests import delete
from common import SSH, delete_all_topics, setup, list_topics, create_topic, delete_topic, describe_topic


def test_topic(setup):
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']

    ssh = SSH(consumer_ip)
    delete_all_topics(ssh, kafka_ip)

    # 토픽 리스트
    topics = list_topics(ssh, kafka_ip, False)
    assert ['__consumer_offsets', 'connect-configs', 'connect-offsets', 'connect-status'] == topics

    # 토픽 생성
    ret = create_topic(ssh, kafka_ip, 'my-topic')
    assert 'Created topic my-topic' in ret
    assert ['my-topic'] == list_topics(ssh, kafka_ip)

    # 토픽 및 파티션 정보
    topic, partitions = describe_topic(ssh, kafka_ip, 'my-topic')
    assert topic[2] == 'PartitionCount: 12'
    assert len(partitions) == 12

    # 토픽 삭제
    ret = delete_topic(ssh, kafka_ip, 'my-topic')
    assert ret == ''
