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
  install_kafka = <<EOT
# Kafka 설치
sudo apt install -y openjdk-8-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.kenv
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.kenv
wget -nv ${var.kafka_url}
kafka_file=${basename(var.kafka_url)}
kafka_dir=$(basename $kafka_file .tgz)
tar xzf $kafka_file
rm $kafka_file
echo "export PATH=$PATH:~/$kafka_dir/bin" >> ~/.kenv
cat ~/.kenv >> ~/.bashrc
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
sudo apt install -y python3-pip unzip

# 코드 설치
git clone --quiet https://github.com/haje01/kfktest.git
cd kfktest && pip3 install -q -r requirements.txt && pip3 install -e .
cd

# Kafka 설치
${var.kafka_url != "" ? local.install_kafka : ""}
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

