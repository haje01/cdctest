provider "aws" {
  region = var.region
}

resource "random_string" "db_passwd" {
  length = 16
  special = true
}

resource "random_string" "mssql_passwd" {
  length = 16
  special = true
}

# MSSQL 보안 그룹
resource "aws_security_group" "mssql" {
  name = "${var.name}-mssql"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 3389
    to_port = 3389
    description = "From Dev PC to Remote Desktop"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 5985
    to_port = 5985
    description = "From Dev PC to WinRM"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Inserter to MSSQL"
    protocol = "tcp"
    security_groups = [
      "${module.inserter.sg_id}"
    ]
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Selector to MSSQL"
    protocol = "tcp"
    security_groups = [
      "${module.selector.sg_id}"
    ]
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Kafka to MSSQL"
    protocol = "tcp"
    security_groups = [
      "${module.kafka.sg_id}"
    ]
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Dev PC to MSSQL"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  egress {
    protocol  = "-1"
    from_port = 0
    to_port   = 0

    cidr_blocks = [
      "0.0.0.0/0",
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-mssql",
      terraform = "true"
    },
    var.tags
  )
}


data "template_file" "initdb" {
  template = file("${path.module}/initdb.tpl")
  vars = {
    user = var.db_user,
    passwd = random_string.db_passwd.result,
    port = var.db_port
  }
}

# MSSQL 인스턴스
resource "aws_instance" "mssql" {
  ami = var.mssql_ami
  instance_type = var.mssql_instance_type
  security_groups = [aws_security_group.mssql.name]
  key_name = var.key_pair_name
  get_password_data = true

  user_data_replace_on_change = true
  user_data = <<EOF
# <powershell>
# # WinRM 설정 (TODO: 안쓰면 제거)
net user ${var.mssql_user} '${random_string.mssql_passwd.result}' /add /y
net localgroup administrators ${var.mssql_user} /add
winrm quickconfig -q
winrm set winrm/config/winrs '@{MaxMemoryPerShellMB="300"}'
winrm set winrm/config '@{MaxTimeoutms="1800000"}'
winrm set winrm/config/service '@{AllowUnencrypted="true"}'
winrm set winrm/config/service/auth '@{Basic="true"}'
netsh advfirewall firewall add rule name="WinRM 5985" protocol=TCP dir=in localport=5985 action=allow
# netsh advfirewall firewall add rule name="WinRM 5986" protocol=TCP dir=in localport=5986 action=allow
# net stop winrm
sc.exe config winrm start=auto
net start winrm

# 로그인 인증을 Mixed 로 변경
Set-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\MSSQL14.MSSQLSERVER\\MSSQLServer' -Name LoginMode -Value 2
Restart-Service -Force MSSQLSERVER

# Chocolatey 설치
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))

# DB 및 유저 초기화
sqlcmd -i C:\\Windows\\Temp\\init.sql
</powershell>
  EOF

  connection {
    type = "winrm"
    host = self.public_ip
    user = var.mssql_user
    password = random_string.mssql_passwd.result
    timeout = "1m"
  }

  provisioner "file" {
    content = data.template_file.initdb.rendered
    destination = "C:/Windows/Temp/init.sql"
  }

  provisioner "remote-exec" {
    inline = [
      # DB 및 유저 초기화
      "sqlcmd -i C:\\Windows\\Temp\\init.sql",
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-mssql",
      terraform = "true"
    },
    var.tags
  )
}

module "inserter" {
  source = "../module/kfktest"
  name = var.name
  nodename = "inserter"
  ubuntu_ami = var.ubuntu_ami
  instance_type = var.insel_instance_type
  private_key = var.private_key
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  kafka_url = ""
  filebeat_url = ""
  tags = var.tags
}

module "selector" {
  source = "../module/kfktest"
  name = var.name
  nodename = "selector"
  ubuntu_ami = var.ubuntu_ami
  instance_type = var.insel_instance_type
  private_key = var.private_key
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  kafka_url = ""
  filebeat_url = ""
  tags = var.tags
}

module "consumer" {
  source = "../module/kfktest"
  name = var.name
  nodename = "consumer"
  ubuntu_ami = var.ubuntu_ami
  instance_type = var.insel_instance_type
  private_key = var.private_key
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  kafka_url = var.kafka_url
  filebeat_url = ""
  tags = var.tags
}

# Kafka
module "kafka" {
  source = "../module/kafka_jdbc"
  name = var.name
  ubuntu_ami = var.ubuntu_ami
  private_key = var.private_key
  kafka_instance_type = var.kafka_instance_type
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  consumer_sg_id = module.consumer.sg_id
  kafka_url = var.kafka_url
  kafka_jdbc_connector = var.kafka_jdbc_connector
  mysql_jdbc_driver = var.mysql_jdbc_driver
  mysql_dbzm_connector = var.mysql_dbzm_connector
  mssql_dbzm_connector = var.mssql_dbzm_connector
  kafka_s3_sink = var.kafka_s3_sink
  timezone = var.timezone
  tags = var.tags
}

