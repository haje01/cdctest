[Unit]
Description=ksqldb

[Service]
Type=simple
User=${user}
ExecStart=/home/ubuntu/${confluent_dir}/bin/ksql-server-start /home/ubuntu/${confluent_dir}/etc/ksqldb/ksql-server.properties
ExecStop=/home/ubuntu/${confluent_dir}/bin/ksql-server-stop
Restart=on-abnormal
Environment="EXTRA_ARGS=-Duser.timezone=${timezone}"
Environment=KSQL_LOG4J_OPTS="-Dlog4j.configuration=file:/home/ubuntu/${confluent_dir}/etc/ksqldb/log4j-rolling.properties"
Environment=LOG_DIR="/var/log/ksql"

[Install]
WantedBy=multi-user.target