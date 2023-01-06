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
sudo apt install -y python3-pip unzip

# 코드 설치
git clone --quiet https://github.com/haje01/kfktest.git
cd kfktest && pip3 install -q -r requirements.txt && pip3 install -e .
cd

# Java 설치
sudo apt install -y openjdk-11-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.kenv
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.kenv

# Confluent Community Edition 설치
wget -qO - https://packages.confluent.io/deb/7.2/archive.key | sudo apt-key add -
sudo add-apt-repository -y "deb [arch=amd64] https://packages.confluent.io/deb/7.2 stable main"
# 현재(2022-11-02) 공식 지원 OS 가 20.x 대 (focal)
sudo add-apt-repository -y "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main"
sudo apt-get install -y confluent-community-2.13
cat ~/.kenv >> ~/.bashrc

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

