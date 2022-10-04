#!/bin/bash

session="kfktest"
tmux new-session -d -s $session

PROFILE="$KFKTEST_PROFILE"
PKEY="$KFKTEST_SSH_PKEY"

export kafka_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.kafka_public_ip.value' | tr -d '"')
export kafka_pip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.kafka_private_ip.value' | tr -d '"')
export prod_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.producer_public_ip.value' | tr -d '"')
export cons_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.consumer_public_ip.value' | tr -d '"')
export ksql_ip=$(cat ~/works/kfktest/temp/$PROFILE/setup.json | jq '.ksqldb_public_ip.value' | tr -d '"')

window=0
tmux rename-window -t $session:$window 'kafka'
tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$kafka_ip -i $PKEY" C-m

window=1
tmux new-window -t $session:$window -n 'producer'
tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$prod_ip -i $PKEY" C-m

window=2
tmux new-window -t $session:$window -n 'consumer'
tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$cons_ip -i $PKEY" C-m

window=3
tmux new-window -t $session:$window -n 'ksqldb'
tmux send-keys -t $session:$window "ssh -o StrictHostKeyChecking=no ubuntu@$ksql_ip -i $PKEY" C-m
tmux send-keys -t $session:$window "sudo ksql http://$kafka_pip:8088" C-m

tmux attach-session -t $session
