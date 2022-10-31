# 보안 그룹
resource "aws_security_group" "kfktest" {
  name = "${var.name}-${var.nodename}"

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
      Name = "${var.name}-${var.nodename}",
      terraform = "true"
    },
    var.tags
  )
}

locals {
# Kafka 설치
  install_kafka = <<EOT
sudo apt install -y openjdk-8-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.kenv
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.kenv
confluent_file=${basename(var.confluent_url)}
confluent_dir=$(basename $confluent_file .zip)
confluent_dir=$(echo $confluent_dir | sed s/-community//)
curl -O ${var.confluent_url}
unzip $confluent_file
rm $confluent_file
echo "export PATH=$PATH:~/$confluent_dir/bin" >> ~/.kenv
cat ~/.kenv >> ~/.bashrc
EOT
# Filebeat 설치
  install_filebeat = <<EOT
curl -L -O ${var.filebeat_url}
filebeat_file=${basename(var.filebeat_url)}
sudo dpkg -i $filebeat_file
rm $filebeat_file
sudo filebeat modules enable kafka
EOT
}

# 인스턴스
resource "aws_instance" "kfktest" {
  ami = var.ubuntu_ami
  instance_type = var.instance_type
  security_groups = [aws_security_group.kfktest.name]
  key_name = var.key_pair_name

  connection {
    type = "ssh"
    host = self.public_ip
    user = "ubuntu"
    private_key = file(var.private_key)
    agent = false
  }

  provisioner "remote-exec" {
    inline = [<<EOT
cloud-init status --wait
sudo apt update
sudo sed -i 's/#$nrconf{restart} = '"'"'i'"'"';/$nrconf{restart} = '"'"'a'"'"';/g' /etc/needrestart/needrestart.conf
sudo sed -i 's/#MaxSessions 10/MaxSessions 200/g' /etc/ssh/sshd_config
sudo service sshd restart
sudo apt install -y python3-pip unzip

# 코드 설치
git clone --quiet https://github.com/haje01/kfktest.git
cd kfktest && pip3 install -q -r requirements.txt && pip3 install -e .
cd

# Kafka 설치
${var.confluent_url != "" ? local.install_kafka : ""}
# Filebeat 설치
${var.filebeat_url != "" ? local.install_filebeat : ""}
EOT
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-${var.nodename}",
      terraform = "true"
    },
    var.tags
  )
}

