import time
import json
import argparse
import logging
from logging.handlers import RotatingFileHandler

from kfktest.util import (linfo, gen_fake_data)

# CLI 용 파서
parser = argparse.ArgumentParser(description="대상 파일에 가짜 로그 생성.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument('dest_file', type=str)
parser.add_argument('-m', '--messages', type=int, default=10000, help="생성할 메시지 수.")
parser.add_argument('-l', '--latency', type=int, default=None, help="메시지당 지연 시간 (ms)")

#----------------------------------------------------------------------
def create_rotating_log(path):
    """
    Creates a rotating log
    """
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)

    # add a rotating handler
    handler = RotatingFileHandler(path, maxBytes=1024*1024,
                                  backupCount=5)
    logger.addHandler(handler)
    return logger


def logger(
        dest_file,
        messages=parser.get_default('messages'),
        latency=parser.get_default('latency')
        ):
    """대상 파일에 가짜 로그 생성."""
    linfo(f"[ ] logger produces {messages} messages to {dest_file}.")
    log = create_rotating_log(dest_file)
    st = time.time()
    for i, data in enumerate(gen_fake_data(messages)):
        log.info(json.dumps(data))
        if (i + 1) % 500 == 0:
            linfo(f"gen {i + 1} th fake data")

        if latency is not None:
            time.sleep(latency / 1000)

    vel = messages / (time.time() - st)
    linfo(f"[v] logger write {messages} messages to {dest_file}. {int(vel)} rows per seconds.")


if __name__ == '__main__':
    args = parser.parse_args()
    logger(args.dest_file, args.messages, args.latency)