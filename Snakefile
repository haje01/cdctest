rule deploy:
    output: "temp/deploy.json"
    shell:
        """
        cd deploy
        terraform apply -var-file=test.tfvars -auto-approve
        terraform output -json > ../{output}
        """

rule copy_deploy:
    input:
        "temp/deploy.json"
    output:
        "temp/copy_deploy"
    shell:
        """
        ip=$(cat temp/deploy.json | jq -r .debezium_public_ip.value)
        pkey=$(cat temp/deploy.json | jq -r .private_key_path.value)
        ssh ubuntu@$ip -i $pkey "sudo chown ubuntu:ubuntu -R dbztest && mkdir -p dbztest/temp && touch dbztest/temp/copy_deploy"
        scp -o "StrictHostKeyChecking=no" -i $pkey {input} ubuntu@$ip:dbztest/{input}
        touch {output}
        """

rule gen_fake_data:
    input:
        "temp/deploy.json",
        "temp/copy_deploy"
    output:
        "temp/result.txt"
    shell:
        "python gen_fake_data.py {input[0]} 1 && touch {output}"

rule remove:
    output:
        "temp/remove"
    shell:
        """
        cd deploy
        terraform destroy -var-file=test.tfvars -auto-approve
        rm -fr temp
        touch ../{output}
        """