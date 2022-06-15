#!/usr/bin/env python
import os

from distutils.core import setup

setup(
    name='kfktest',
    version="0.0.1",
    author="JeongJu Kim",
    author_email='haje01@gmail.com',
    url="https://github.com/haje01/kfktest",
    description="Kafka Test",
    license='MIT License',
    install_requires=[
        'pymssql',
        'mysql-connector-python',
        'faker',
        'kafka-python',
        'pytest',
        'paramiko',
    ]
)
