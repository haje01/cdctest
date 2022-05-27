#!/bin/bash
curl --location --request POST 'http://localhost:8083/connectors' \
    --header 'Content-Type: application/json' \
    --data-raw '{
    "name" : "my-source-connect",
    "config" : {
    "connector.class" : "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url":"jdbc:${host}://localhost:3306/test",
    "connection.user":"${user}",
    "connection.password":"${passwd}",
    "mode": "incrementing",
    "incrementing.column.name" : "id",
    "table.whitelist":"person",
    "topic.prefix" : "my_topic_",
    "tasks.max" : "1"
    }
}'