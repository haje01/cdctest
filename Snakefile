rule:
    output: "temp/deploy.json"
    shell:
        # "terraform apply -var-file=test.tfvars",
        "cd deploy && terraform output -json > ../{output}"

rule:
    input:
        "temp/deploy.json"
    output:
        "temp/result.txt"
    script:
        "gen_fake_data.py"