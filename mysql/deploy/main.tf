provider "aws" {
  region = var.region
}

variable PKEY {
  type = string
}

resource "random_string" "db_passwd" {
  length = 16
  special = true
}

# MySQL 보안 그룹
resource "aws_security_group" "mysql" {
  name = "${var.name}-mysql"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 3306
    to_port = 3306
    description = "From Inserter to MySQL"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.inserter.id}"
    ]
  }

  ingress {
    from_port = 3306
    to_port = 3306
    description = "From Selector to MySQL"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.selector.id}"
    ]
  }

  ingress {
    from_port = 3306
    to_port = 3306
    description = "From Kafka to MySQL"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.kafka.id}"
    ]
  }

  ingress {
    from_port = 3306
    to_port = 3306
    description = "From Dev PC to MySQL"
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
      Name = "${var.name}-mysql",
      terraform = "true"
    },
    var.tags
  )
}

data "template_file" "initdb" {
  template = file("${path.module}/initdb.tpl")
  vars = {
    user = var.db_user,
    passwd = random_string.db_passwd.result
  }
}

# MySQL 인스턴스
resource "aws_instance" "mysql" {
  ami = var.ubuntu_ami
  instance_type = var.mysql_instance_type
  security_groups = [aws_security_group.mysql.name]
  key_name = var.key_pair_name

  connection {
    type = "ssh"
    host = self.public_ip
    user = "ubuntu"
    private_key = file(var.PKEY)
    agent = false
  }

  provisioner "file" {
    content = data.template_file.initdb.rendered
    destination = "/tmp/init.sql"
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "sudo apt update",
      "sudo apt install -y mysql-server",
      "sleep 5",
      "sudo sed -i 's/bind-address.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf",
      "sudo service mysql stop",
      "sudo service mysql start",
      # MySQL 기동 대기
      # "while ! sudo mysql -e 'SELECT 1' > /dev/null 2>&1 ; do sleep 1 ; done",
      "sleep 5",
      # DB 및 유저 초기화
      "sudo mysql < /tmp/init.sql"
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-mysql",
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
    private_key = file(var.PKEY)
    agent = false
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "sudo apt update",
      "sudo apt install -y python3-pip",
      "git clone --quiet https://github.com/haje01/kfktest.git",
      "cd kfktest && pip3 install -q -r requirements.txt"
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
    private_key = file(var.PKEY)
    agent = false
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "sudo apt update",
      "sudo apt install -y python3-pip",
      "git clone --quiet https://github.com/haje01/kfktest.git",
      "cd kfktest && pip3 install -q -r requirements.txt"
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
#     host = aws_instance.mysql.private_ip,
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
    private_key = file(var.PKEY)
    agent = false
  }

  # Kafka Connector 복사
  provisioner "file" {
    source = var.kafka_jdbc_connector
    destination = "/tmp/${basename(var.kafka_jdbc_connector)}"
  }

  # MySQL JDBC Driver 복사
  provisioner "file" {
    source = var.mysql_jdbc_driver
    destination = "/tmp/${basename(var.mysql_jdbc_driver)}"
  }

  # Kafka JDBC Connector 등록 스크립트
  # provisioner "file" {
  #   content = data.template_file.regconn.rendered
  #   destination = "/tmp/regconn.sh"
  # }

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

      # MySQL JDBC Driver 설치
      "sudo apt install -y /tmp/${basename(var.mysql_jdbc_driver)}",
      "cp /usr/share/java/mysql-connector-java-*.jar ~/$kafka_dir/connectors/$kjc_dir/lib",

      # 실행
      "pwd > /tmp/pwd",
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
    private_key = file(var.PKEY)
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
      "git clone --quiet https://github.com/haje01/kfktest.git",
      "cd kfktest && pip3 install -q -r requirements.txt"
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
