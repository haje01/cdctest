[Unit]
Description=kafka-connect
Requires=kafka.service
After=kafka.service

[Service]
Type=simple
User=${user}
ExecStart=/home/ubuntu/${kafka_dir}/bin/connect-distributed.sh /home/ubuntu/${kafka_dir}/config/connect-distributed.properties
ExecStop=pkill -ef ConnectDistributed
Restart=on-abnormal
Environment="EXTRA_ARGS=-Duser.timezone=${timezone}"
SuccessExitStatus=143
# syslog 에서 alert 하는 경우를 우회하기 위해
# StandardOutput=append:/var/log//kafka-connect.log
# StandardError=append:/tmp/kafka-connect.log

[Install]
WantedBy=multi-user.target