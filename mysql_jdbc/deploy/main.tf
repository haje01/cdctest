provider "aws" {
  region = var.region
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
  template = file("${path.module}/init.tpl")
  vars = {
    user = var.db_user,
    passwd = var.db_passwd
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
    private_key = file(var.private_key_path)
    agent = false
  }

  provisioner "file" {
    content = data.template_file.initdb.rendered
    destination = "/tmp/init.sql"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt update -qq",
      "sudo apt --fix-broken install",
      "sudo apt install -qq -y mysql-server",
      "sleep 5",
      "sudo sed -i 's/bind-address.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf",
      "sudo service mysql stop",
      "sudo service mysql start",
      # MySQL 기동 대기
      "while ! sudo mysql -e 'SELECT 1' > /dev/null 2>&1 ; do sleep 1 ; done",
      # "sleep 5",
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
    private_key = file(var.private_key_path)
    agent = false
  }

  provisioner "remote-exec" {
    inline = [
      "sudo add-apt-repository -y universe",
      "sudo apt update -qq",
      "sudo apt install -qq -y python3-pip",
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
      "sudo add-apt-repository -y universe",
      "sudo apt update -qq",
      "sudo apt install -qq -y python3-pip",
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

  provisioner "file" {
    source = var.kafka_connect_jdbc_path
    destination = "/tmp/${basename(var.kafka_connect_jdbc_path)}"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt update -qq",
      # Kafka 설치
      "sudo apt -qq -y install openjdk-8-jdk",
      "echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> /etc/profile",
      "echo 'export PATH=$PATH:$JAVA_HOME/bin' >> /etc/profile",
      "wget -nv https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz",
      "tar xzf kafka_2.13-3.0.0.tgz",
      "rm kafka_2.13-3.0.0.tgz",
      "cd kafka_2.13-3.0.0",
      "screen -S zookeeper -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash'",
      "screen -S kafka -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/kafka-server-start.sh config/server.properties; exec bash'",
      # "sleep 3",  # screen 세션이 죽지 않도록
      # Kafka Connector 설치
      "mv /tmp/${basename(var.kafka_connect_jdbc_path)} /opt/kafka_2.13-3.0.0/connectors",
      "cd /opt/kafka_2.13-3.0.0/connectors",
      "unzip ${basename(var.kafka_connect_jdbc_path)}"
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
