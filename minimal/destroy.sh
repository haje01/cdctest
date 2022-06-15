#!/bin/bash
rm -fr temp
cd deploy
TF_VAR_private_key=$KFKTEST_SSH_PKEY terraform destroy -var-file=test.tfvars -auto-approve
