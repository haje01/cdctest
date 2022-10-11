[Unit]
Description=ksqldb

[Service]
Type=simple
User=${user}
ExecStart=/usr/bin/ksql-server-start /etc/ksqldb/ksql-server.properties
ExecStop=/usr/bin/ksql-server-stop
Restart=on-abnormal
Environment="EXTRA_ARGS=-Duser.timezone=${timezone}"
Environment=KSQL_LOG4J_OPTS="-Dlog4j.configuration=file:/etc/ksqldb/log4j-rolling.properties"
Environment=LOG_DIR="/var/log/ksql"

[Install]
WantedBy=multi-user.target