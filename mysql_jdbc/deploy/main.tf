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

  user_data_replace_on_change = true
  user_data = <<EOF
#!/bin/bash
sudo apt update
sudo apt install -y mysql-server
sudo sed -i "s/bind-address.*/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf
sudo service mysql stop
sudo service mysql start
.cnf
  EOF

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
      # MySQL 기동 대기
      "while ! sudo mysql -e 'SELECT 1' > /dev/null 2>&1 ; do sleep 1 ; done",
      # DB 및 유저 초기화
      "sudo mysql < /tmp/init.sql",
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

  user_data_replace_on_change = true
  user_data = <<EOF
#!/bin/bash
sudo apt update
sudo apt install -y python3-pip
cd /home/ubuntu
git clone https://github.com/haje01/dbztest.git
cd dbztest && pip3 install -r requirements.txt
  EOF

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

# Inserter 인스턴스
resource "aws_instance" "selector" {
  ami = var.ubuntu_ami
  instance_type = var.insel_instance_type
  security_groups = [aws_security_group.selector.name]
  key_name = var.key_pair_name

  user_data_replace_on_change = true
  user_data = <<EOF
#!/bin/bash
sudo apt update
sudo apt install -y python3-pip
cd /home/ubuntu
git clone https://github.com/haje01/dbztest.git
cd dbztest && pip3 install -r requirements.txt
  EOF

  tags = merge(
    {
      Name = "${var.name}-selector",
      terraform = "true"
    },
    var.tags
  )
}
