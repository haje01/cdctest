rule:
    output: "temp/deploy.json"
    shell:
        """
        cd deploy
        terraform apply -var-file=test.tfvars -auto-approve
        terraform output -json > ../{output}
        """

rule:
    input:
        "temp/deploy.json"
    output:
        "temp/result.txt"
    script:
        "gen_fake_data.py"

rule:
    output:
        "temp/remove"
    shell:
        """
        cd deploy
        terraform destroy -var-file=test.tfvars -auto-approve
        rm -fr temp
        touch ../{output}
        """