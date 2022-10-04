#!/bin/bash

session="kfk-local"
tmux new-session -d -s $session

PROFILE="$KFKTEST_PROFILE"

window=0
tmux rename-window -t $session:$window 'deploy'
tmux send-keys -t $session:$window 'cd ~/works/kfktest' C-m
tmux send-keys -t $session:$window 'pyenv activate kfktest' C-m
tmux send-keys -t $session:$window 'snakemake -f temp/$PROFILE/setup.json -j'

window=1
tmux new-window -t $session:$window -n 'tests'
tmux send-keys -t $session:$window 'cd ~/works/kfktest/tests' C-m
tmux send-keys -t $session:$window 'pyenv activate kfktest' C-m

window=2
tmux new-window -t $session:$window -n 'kfktest'
tmux send-keys -t $session:$window 'cd ~/works/kfktest' C-m
tmux send-keys -t $session:$window 'pyenv activate kfktest' C-m

tmux attach-session -t $session
