[Unit]
Description=kafka-connect
Requires=kafka.service
After=kafka.service

[Service]
Type=simple
User=${user}
ExecStart=/home/${user}/${kafka_dir}/bin/connect-distributed.sh /home/${user}/${kafka_dir}/config/connect-distributed.properties

[Install]
WantedBy=multi-user.target