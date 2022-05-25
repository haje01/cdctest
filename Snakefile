INSERTER = 5
SELECTOR = 5
BATCH = 100
EPOCH = 100

rule setup:
    """시스템 설치."""
    output: "temp/setup.json"
    shell:
        """
        cd deploy
        terraform apply -var-file=test.tfvars -auto-approve
        terraform output -json > ../{output}
        """

rule copy_deploy:
    """디플로이 정보 복사."""
    input:
        "temp/setup.json"
    output:
        "temp/copy_deploy"
    shell:
        """
        ip=$(cat temp/setup.json | jq -r .debezium_public_ip.value)
        pkey=$(cat temp/setup.json | jq -r .private_key_path.value)
        ssh -o "StrictHostKeyChecking=no" ubuntu@$ip -i $pkey "sudo chown ubuntu:ubuntu -R dbztest && mkdir -p dbztest/temp && touch dbztest/temp/copy_deploy"
        scp -i $pkey {input} ubuntu@$ip:dbztest/{input}
        touch {output}
        """

rule inserter:
    """더미 데이터 인서트.

    인서트 전용 노드에서 실행.

    """
    input:
        "temp/setup.json",
        "temp/copy_deploy",
        "temp/reset_table"
    output:
        "temp/inserter_{pid}.txt"
    params:
        pid="{pid}",
        epoch=EPOCH,
        batch=BATCH
    shell:
        """
        ip=$(cat temp/setup.json | jq -r .debezium_public_ip.value)
        pkey=$(cat temp/setup.json | jq -r .private_key_path.value)
        ssh ubuntu@$ip -i $pkey "cd dbztest && python3 inserter.py temp/setup.json {params.pid} {params.epoch} {params.batch} > {output}"
        scp -i $pkey ubuntu@$ip:dbztest/{output} {output}
        """


rule selector:
    """더미 데이터 셀렉트.

    셀렉트 전용 노드에서 실행

    """
    input:
        "temp/setup.json",
        "temp/copy_deploy"
    output:
        "temp/selector_{pid}.txt"
    params:
        pid="{pid}"
    shell:
        """
        ip=$(cat temp/setup.json | jq -r .debezium_public_ip.value)
        pkey=$(cat temp/setup.json | jq -r .private_key_path.value)
        ssh ubuntu@$ip -i $pkey "cd dbztest && python3 selector.py temp/setup.json {params.pid} > {output}"
        scp -i $pkey ubuntu@$ip:dbztest/{output} {output}
        """


rule reset_table:
    """인서트용 테이블 초기화."""
    input:
        "temp/setup.json"
    output:
        "temp/reset_table"
    script:
        "reset_table.py"


rule start_time:
    """테스트 시작 시간."""
    output:
        "temp/start_time"
    shell:
        "date +%s > {output}"


rule result:
    """테스트 결과."""
    input:
        "temp/start_time",
        expand("temp/inserter_{pid}.txt", pid=(range(INSERTER))),
        expand("temp/selector_{pid}.txt", pid=(range(SELECTOR)))
    output:
        "temp/result.txt"
    script:
        "result.py"


rule clear:
    """테스트 결과물 제거."""
    output:
        "temp/clear"
    shell:
        """
        ip=$(cat temp/setup.json | jq -r .debezium_public_ip.value)
        pkey=$(cat temp/setup.json | jq -r .private_key_path.value)
        ssh ubuntu@$ip -i $pkey "pkill python3" || true
        rm -f temp/reset_table
        rm -f temp/inserter_*.txt
        rm -f temp/selector_*.txt
        rm -f temp/result.txt
        rm -f temp/start_time
        touch {output}
        """

rule destroy:
    """시스템 제거."""
    output:
        "temp/destroy"
    shell:
        """
        rm -fr temp
        cd deploy
        terraform destroy -var-file=test.tfvars -auto-approve
        touch ../{output}
        """