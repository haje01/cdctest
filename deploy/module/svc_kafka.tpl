[Unit]
Description=kafka
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=${user}
ExecStart=/home/ubuntu/${kafka_dir}/bin/kafka-server-start.sh /home/ubuntu/${kafka_dir}/config/server.properties
ExecStop=/home/ubuntu/${kafka_dir}/bin/kafka-server-stop.sh
Restart=on-abnormal
Environment="EXTRA_ARGS=-Duser.timezone=${timezone}"
# syslog 에서 alert 하는 경우를 우회하기 위해
# StandardOutput=append:/var/log/kafka.log
# StandardError=append:/var/log/kafka.log

[Install]
WantedBy=multi-user.target