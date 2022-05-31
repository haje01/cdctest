#!/bin/bash

# TODO:
# "key.converter": "io.confluent.connect.avro.AvroConverter",
# "value.converter": "io.confluent.connect.avro.AvroConverter",
# "key.converter.schema.registry.url": ""
# "value.converter.schema.registry.url": ""

curl --location --request POST 'http://localhost:8083/connectors' \
    --header 'Content-Type: application/json' \
    --data-raw '{
    "name" : "my-source-connect",
    "config" : {
        "connector.class" : "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url":"jdbc:mysql://${host}:3306/test",
        "connection.user":"${user}",
        "connection.password":"${passwd}",
        "mode": "incrementing",
        "incrementing.column.name" : "id",
        "table.whitelist":"person",
        "topic.prefix" : "my_topic_",
        "tasks.max" : "1"
    }
}'