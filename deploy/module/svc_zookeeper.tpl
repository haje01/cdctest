[Unit]
Description=zookeeper
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=${user}
ExecStart=/home/${user}/${kafka_dir}/bin/zookeeper-server-start.sh /home/${user}/${kafka_dir}/config/zookeeper.properties
ExecStop=/home/${user}/${kafka_dir}/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target