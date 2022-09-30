[Unit]
Description=ksqldb

[Service]
Type=simple
User=${user}
ExecStart=/usr/bin/ksql-server-start /etc/ksqldb/ksql-server.properties
ExecStop=/usr/bin/ksql-server-stop
Restart=on-abnormal
Environment="EXTRA_ARGS=-Duser.timezone=${timezone}"

[Install]
WantedBy=multi-user.target