import os
import argparse

from kfktest.util import HOME, scp_to_remote, load_setup

# CLI 용 파서
parser = argparse.ArgumentParser(description="인프라 정보 파일 원격으로 복사.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('profile', type=str, help="프로파일 이름.")


def cp_setup(profile):
    """프로파일에 맞는 인프라 정보 파일 원격 서버에 복사."""
    setup = load_setup(profile)

    if profile in ('mysql', 'mssql'):
        targets = ['consumer_public_ip', 'inserter_public_ip', 'selector_public_ip']
    else:
        targets = ['producer_public_ip', 'consumer_public_ip']

    for target in targets:
        ip = setup[target]['value']
        src = os.path.join(HOME, f'temp/{profile}/setup.json')
        scp_to_remote(src, ip, f'~/kfktest/temp/{profile}')


if __name__ == '__main__':
    args = parser.parse_args()
    cp_setup(args.profile)