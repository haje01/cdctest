#!/bin/bash

session="kfk-remote"
tmux kill-session -t $session
tmux new-session -d -s $session

PROFILE="$KFKTEST_PROFILE"
PKEY="$KFKTEST_SSH_PKEY"

echo "profile $PROFILE"

export kafka_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.kafka_public_ip.value' | tr -d '"')
export kafka_pip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.kafka_private_ip.value' | tr -d '"')
export prod_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.producer_public_ip.value' | tr -d '"')
export cons_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.consumer_public_ip.value' | tr -d '"')
export mysql_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.mysql_public_ip.value' | tr -d '"')
export ksql_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.ksqldb_public_ip.value' | tr -d '"')

echo "ksql_ip $ksl_ip"

window=0
tmux rename-window -t $session:$window 'kafka'
tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$kafka_ip -i $PKEY" C-m

if [ $prod_ip 1= "null" ]; then
    window=1
    tmux new-window -t $session:$window -n 'producer'
    tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$prod_ip -i $PKEY" C-m
fi

window=2
tmux new-window -t $session:$window -n 'consumer'
tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$cons_ip -i $PKEY" C-m

if [ $ksql_ip != "null" ]; then
    window=3
    tmux new-window -t $session:$window -n 'ksql-cli'
    tmux send-keys -t $session:$window "sudo ksql http://${ksql_ip}:8088" C-m

    window=4
    tmux new-window -t $session:$window -n 'ksqldb'
    tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$ksql_ip -i $PKEY" C-m
fi

if [ $mysql_ip != "null" ]; then
    window=5
    tmux new-window -t $session:$window -n 'mysql'
    tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$mysql_ip -i $PKEY" C-m
fi


tmux attach-session -t $session
