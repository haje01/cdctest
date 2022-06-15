#!/bin/bash
mkdir -p temp
cd deploy
TF_VAR_private_key=$KFKTEST_SSH_PKEY terraform apply -var-file=test.tfvars -auto-approve
terraform output -json > ../temp/setup.json
