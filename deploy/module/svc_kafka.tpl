[Unit]
Description=kafka
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=${user}
ExecStart=EXTRA_ARGS="-Duser.timezone=${timezone} /home/${user}/${kafka_dir}/bin/kafka-server-start.sh /home/${user}/${kafka_dir}/config/server.properties
ExecStop=/home/${user}/${kafka_dir}/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target