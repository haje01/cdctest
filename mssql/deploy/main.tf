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
      "${aws_security_group.inserter.id}"
    ]
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Selector to MSSQL"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.selector.id}"
    ]
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Kafka to MSSQL"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.kafka.id}"
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

# Inserter 보안 그룹
resource "aws_security_group" "inserter" {
  name = "${var.name}-inserter"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
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
      Name = "${var.name}-inserter",
      terraform = "true"
    },
    var.tags
  )
}

# Inserter 인스턴스
resource "aws_instance" "inserter" {
  ami = var.ubuntu_ami
  instance_type = var.insel_instance_type
  security_groups = [aws_security_group.inserter.name]
  key_name = var.key_pair_name

  connection {
    type = "ssh"
    host = self.public_ip
    user = "ubuntu"
    private_key = file(var.private_key_path)
    agent = false
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "sudo apt update",
      "sudo apt install -y python3-pip",
      "git clone --quiet https://github.com/haje01/cdctest.git",
      "cd cdctest && pip3 install -q -r requirements.txt"
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-inserter",
      terraform = "true"
    },
    var.tags
  )
}

# Selector 보안 그룹
resource "aws_security_group" "selector" {
  name = "${var.name}-selector"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
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
      Name = "${var.name}-selector",
      terraform = "true"
    },
    var.tags
  )
}

# Selector 인스턴스
resource "aws_instance" "selector" {
  ami = var.ubuntu_ami
  instance_type = var.insel_instance_type
  security_groups = [aws_security_group.selector.name]
  key_name = var.key_pair_name

  connection {
    type = "ssh"
    host = self.public_ip
    user = "ubuntu"
    private_key = file(var.private_key_path)
    agent = false
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "sudo add-apt-repository -y universe",
      "sudo apt update",
      "sudo apt install -y python3-pip",
      "git clone --quiet https://github.com/haje01/cdctest.git",
      "cd cdctest && pip3 install -q -r requirements.txt"
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-selector",
      terraform = "true"
    },
    var.tags
  )
}


# Kafka JDBC Connector 등록
# data "template_file" "regconn" {
#   template = file("${path.module}/regconn.tpl")
#   vars = {
#     host = aws_instance.mssql.private_ip,
#     user = var.db_user,
#     passwd = random_string.db_passwd.result,
#     port = var.db_port
#   }
# }

# Kafka 보안 그룹
resource "aws_security_group" "kafka" {
  name = "${var.name}-kafka"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 9092
    to_port = 9092
    description = "From Dev PC to kafka"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 9092
    to_port = 9092
    description = "From Consumer to kafka"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.consumer.id}"
    ]
  }

  ingress {
    from_port = 8083
    to_port = 8083
    description = "From Consumer to kafka API"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.consumer.id}"
    ]
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
      Name = "${var.name}-kafka",
      terraform = "true"
    },
    var.tags
  )
}

# Kafka 인스턴스
resource "aws_instance" "kafka" {
  ami = var.ubuntu_ami
  instance_type = var.kafka_instance_type
  security_groups = [aws_security_group.kafka.name]
  key_name = var.key_pair_name

connection {
    type = "ssh"
    host = self.public_ip
    user = "ubuntu"
    private_key = file(var.private_key_path)
    agent = false
  }

  # Kafka Connector 복사
  provisioner "file" {
    source = var.kafka_jdbc_connector
    destination = "/tmp/${basename(var.kafka_jdbc_connector)}"
  }

  # # MSSQL JDBC Driver 복사
  # provisioner "file" {
  #   source = var.mssql_jdbc_driver
  #   destination = "/tmp/${basename(var.mssql_jdbc_driver)}"
  # }

  # Kafka JDBC Connector 등록 스크립트
  # provisioner "file" {
  #   content = data.template_file.regconn.rendered
  #   destination = "/tmp/regconn.sh"
  # # }
#
#   provisioner "file" {
#     # 설치 후 바로는 커넥터 등록이 잘 안되어 재시도 하게
#     content = <<EOT
# #!/bin/bash
# while ! curl -s localhost:8083/connectors >> /dev/null 2>&1 ; do sleep 5 ; done
# cons=$(curl -s localhost:8083/connectors)
# while [ "$cons" != "[\"my-source-connect\"]" ]
# do
#   sleep 5
#   bash /tmp/regconn.sh
#   cons=$(curl -s localhost:8083/connectors)
# done
# EOT
#     destination = "/tmp/regretry.sh"
#   }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "sudo apt update",
      "sudo apt install -y unzip",

      # Kafka 설치
      "sudo apt install -y openjdk-8-jdk",
      "echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.myenv",
      "echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.myenv",
      "wget -nv ${var.kafka_url}",
      "kafka_file=${basename(var.kafka_url)}",
      "kafka_dir=$(basename $kafka_file .tgz)",
      "tar xzf $kafka_file",
      "rm $kafka_file",
      "cd $kafka_dir",
      "sed -i \"s/#advertised.listeners=PLAINTEXT:\\/\\/your.host.name/advertised.listeners=PLAINTEXT:\\/\\/${self.private_ip}/\" config/server.properties",
      # "echo 'delete.topic.enable=true' >> config/server.properties",

      "echo \"export PATH=$PATH:~/$kafka_dir/bin\" >> ~/.myenv",
      "cat ~/.myenv >> ~/.bashrc",

      # Kafka JDBC Connector 설치
      "mkdir -p connectors",
      "mv /tmp/${basename(var.kafka_jdbc_connector)} connectors/",
      "cd connectors/",
      "unzip ${basename(var.kafka_jdbc_connector)}",
      "rm ${basename(var.kafka_jdbc_connector)}",
      "kjc_file=${basename(var.kafka_jdbc_connector)}",
      "kjc_dir=$(basename $kjc_file .zip)",
      "sed -i \"s/#plugin.path=/plugin.path=\\/home\\/ubuntu\\/$kafka_dir\\/connectors/\" ../config/connect-distributed.properties",
      "cd ..",

      # MSSQL JDBC Driver 설치 는 confluentinc-kafka-connect-jdbc 에 이미 포함되어 있기에 생략
      # "cp /tmp/${basename(var.mssql_jdbc_driver)} ~/$kafka_dir/connectors/$kjd_dir/lib",

      # 실행
      "screen -S zookeeper -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash'",
      "screen -S kafka -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/kafka-server-start.sh config/server.properties; exec bash'",
      "screen -S kafka-connect -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/connect-distributed.sh config/connect-distributed.properties; exec bash'",
      "while ! curl -s localhost:8083/connectors >> /dev/null 2>&1 ; do sleep 5 ; done",

      # Connector 등록
      # "bash /tmp/regretry.sh",
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-kafka",
      terraform = "true"
    },
    var.tags
  )
}

# Consumer 보안 그룹
resource "aws_security_group" "consumer" {
  name = "${var.name}-consumer"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
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
      Name = "${var.name}-consumer",
      terraform = "true"
    },
    var.tags
  )
}

# Consumer 인스턴스
resource "aws_instance" "consumer" {
  ami = var.ubuntu_ami
  instance_type = var.insel_instance_type
  security_groups = [aws_security_group.consumer.name]
  key_name = var.key_pair_name

  connection {
    type = "ssh"
    host = self.public_ip
    user = "ubuntu"
    private_key = file(var.private_key_path)
    agent = false
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "sudo apt update",

      # Kafka 설치
      "sudo apt install -y openjdk-8-jdk",
      "echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.myenv",
      "echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.myenv",
      "wget -nv ${var.kafka_url}",
      "kafka_file=${basename(var.kafka_url)}",
      "kafka_dir=$(basename $kafka_file .tgz)",
      "tar xzf $kafka_file",
      "rm $kafka_file",
      "cd $kafka_dir",
      "echo \"export PATH=$PATH:~/$kafka_dir/bin\" >> ~/.myenv",
      "cat ~/.myenv >> ~/.bashrc",

      # 코드 설치
      "sudo apt install -y python3-pip",
      "git clone --quiet https://github.com/haje01/cdctest.git",
      "cd cdctest && pip3 install -q -r requirements.txt"
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-consumer",
      terraform = "true"
    },
    var.tags
  )
}
