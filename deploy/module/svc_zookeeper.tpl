[Unit]
Description=zookeeper
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=${user}
ExecStart=/home/ubuntu/${confluent_dir}/bin/zookeeper-server-start /home/ubuntu/${confluent_dir}/etc/kafka/zookeeper.properties
ExecStop=/home/ubuntu/${confluent_dir}/bin/zookeeper-server-stop
Restart=on-abnormal
# syslog 에서 alert 하는 경우를 우회하기 위해
# StandardOutput=append:/var/log/zookeeper.log
# StandardError=append:/var/log/zookeeper.log

[Install]
WantedBy=multi-user.target