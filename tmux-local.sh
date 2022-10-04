#!/bin/bash

session="kfktest"
tmux new-session -d -s $session

PROFILE="${KFKPROF:-nodb}"

window=0
tmux rename-window -t $session:$window 'deploy'
tmux send-keys -t $session:$window 'cd ~/works/kfktest' C-m
tmux send-keys -t $session:$window 'pyenv activate kfktest' C-m
tmux send-keys -t $session:$window 'snakemake -f temp/$PROFILE/setup.json -j'

window=1
tmux new-window -t $session:$window -n 'test'
tmux send-keys -t $session:$window 'cd ~/works/kfktest/test' C-m
tmux send-keys -t $session:$window 'pyenv activate kfktest' C-m

window=2
tmux new-window -t $session:$window -n 'kfktest'
tmux send-keys -t $session:$window 'cd ~/works/kfktest' C-m
tmux send-keys -t $session:$window 'pyenv activate kfktest' C-m

tmux attach-session -t $session
