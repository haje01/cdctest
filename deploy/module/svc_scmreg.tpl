[Unit]
Description=schema-registry

[Service]
Type=simple
User=${user}
ExecStart=/home/ubuntu/${confluent_dir}/bin/schema-registry-start /home/ubuntu/${confluent_dir}/etc/schema-registry/schema-registry.properties
ExecStop=/home/ubuntu/${confluent_dir}/bin/schema-registry-stop
Restart=on-abnormal
Environment="EXTRA_ARGS=-Duser.timezone=${timezone}"

[Install]
WantedBy=multi-user.target