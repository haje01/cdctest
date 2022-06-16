
rule setup:
    """프로파일별 인프라 설치."""
    output: "temp/{profile}/setup.json"
    shell:
        """
        cd deploy/{wildcards.profile}
        TF_VAR_private_key=$KFKTEST_SSH_PKEY terraform apply -var-file=test.tfvars -auto-approve
        terraform output -json > ../../{output}
        """


rule destroy:
    """시스템 제거."""
    output:
        "temp/{profile}/destroy"
    shell:
        """
        cd deploy/{wildcards.profile}
        TF_VAR_private_key=$KFKTEST_SSH_PKEY terraform destroy -var-file=test.tfvars -auto-approve
        touch ../../{output}
        """