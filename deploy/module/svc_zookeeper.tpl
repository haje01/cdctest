[Unit]
Description=zookeeper
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=${user}
ExecStart=/home/ubuntu/${kafka_dir}/bin/zookeeper-server-start.sh /home/ubuntu/${kafka_dir}/config/zookeeper.properties
ExecStop=/home/ubuntu/${kafka_dir}/bin/zookeeper-server-stop.sh
Restart=on-abnormal
# syslog 에서 alert 하는 경우를 우회하기 위해
StandardOutput=append:/var/log/zookeeper.log
StandardError=append:/var/log/zookeeper.log

[Install]
WantedBy=multi-user.target