provider "aws" {
  region = var.region
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
      "${module.inserter.sg_id}"
    ]
  }

  ingress {
    from_port = 3306
    to_port = 3306
    description = "From Selector to MySQL"
    protocol = "tcp"
    security_groups = [
      "${module.selector.sg_id}"
    ]
  }

  ingress {
    from_port = 3306
    to_port = 3306
    description = "From Kafka to MySQL"
    protocol = "tcp"
    security_groups = [
      "${module.kafka.sg_id}"
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
    private_key = file(var.private_key)
    agent = false
  }

  provisioner "file" {
    content = data.template_file.initdb.rendered
    destination = "/tmp/init.sql"
  }

  provisioner "remote-exec" {
    inline = [<<EOT
cloud-init status --wait
sudo apt update
sudo apt install -y mysql-server
sleep 5
sudo sed -i 's/bind-address.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf
sudo service mysql stop
sudo service mysql start
# MySQL 기동 대기
# while ! sudo mysql -e 'SELECT 1' > /dev/null 2>&1 ; do sleep 1 ; done
sleep 5
# DB 및 유저 초기화
sudo mysql < /tmp/init.sql
EOT
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

# Inserter / Select
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
  tags = var.tags
}

# Kafka
module "kafka" {
  source = "../../module/kafka"
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
  tags = var.tags
}

