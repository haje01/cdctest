[Unit]
Description=kafka
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=${user}
ExecStart=/home/ubuntu/${confluent_dir}/bin/kafka-server-start /home/ubuntu/${confluent_dir}/etc/kafka/server.properties
ExecStop=/home/ubuntu/${confluent_dir}/bin/kafka-server-stop
LimitNOFILE=1000000
TimeoutStopSec=180
Restart=on-abnormal
Environment="EXTRA_ARGS=-Duser.timezone=${timezone}"
# syslog 에서 alert 하는 경우를 우회하기 위해
# StandardOutput=append:/var/log/kafka.log
# StandardError=append:/var/log/kafka.log

[Install]
WantedBy=multi-user.target