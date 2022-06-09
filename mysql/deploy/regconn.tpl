#!/bin/bash

# TODO:
# "key.converter": "io.confluent.connect.avro.AvroConverter",
# "value.converter": "io.confluent.connect.avro.AvroConverter",
# "key.converter.schema.registry.url": ""
# "value.converter.schema.registry.url": ""

curl -vs -X POST 'http://localhost:8083/connectors' \
    -H 'Content-Type: application/json' \
    --data-raw '{
    "name" : "my-source-connect",
    "config" : {
        "connector.class" : "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url":"jdbc:mysql://${host}:${port}/test",
        "connection.user":"${user}",
        "connection.password":"${passwd}",
        "mode": "incrementing",
        "incrementing.column.name" : "id",
        "table.whitelist":"person",
        "topic.prefix" : "my_topic_",
        "poll.interval.ms": "5000",
        "tasks.max" : "1"
    }
}'