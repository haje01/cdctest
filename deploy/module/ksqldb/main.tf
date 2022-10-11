# 보안 그룹
resource "aws_security_group" "ksqldb" {
  name = "${var.name}-${var.nodename}"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 8088
    to_port = 8088
    description = "From Dev PC to ksqlDB"
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
  install_ksqldb = <<EOT
## ksqlDB 설치
sudo apt install -y software-properties-common curl gnupg
# Import the public key
curl -sq http://ksqldb-packages.s3.amazonaws.com/deb/0.27/archive.key | sudo apt-key add -
# Add the ksqlDB apt repository
sudo add-apt-repository -y "deb http://ksqldb-packages.s3.amazonaws.com/deb/0.27 stable main"
sudo apt update

sudo apt install confluent-ksqldb
sudo sed -i 's/bootstrap.servers=localhost:9092/bootstrap.servers=${var.kafka_private_ip}:9092/' /etc/ksqldb/ksql-server.properties
# 토픽 처음부터
sudo echo 'ksql.streams.auto.offset.reset=earliest' >> /etc/ksqldb/ksql-server.properties
# Timeout 에러 방지
echo 'ksql.streams.shutdown.timeout.ms=30000' | sudo tee -a /etc/ksqldb/ksql-server.properties
echo 'ksql.idle.connection.timeout.seconds=30000' | sudo tee -a /etc/ksqldb/ksql-server.properties
EOT
}

# 인스턴스
resource "aws_instance" "ksqldb" {
  ami = var.ubuntu_ami
  instance_type = var.instance_type
  security_groups = [aws_security_group.ksqldb.name]
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

sudo apt install -y openjdk-8-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc

cat <<EOF > ~/.tmux.conf
set -g mouse on
set-option -g status-right ""
set-option -g history-limit 10000
set-window-option -g mode-keys vi
bind-key -T copy-mode-vi v send -X begin-selection
bind-key -T copy-mode-vi V send -X select-line
bind-key -T copy-mode-vi y send -X copy-pipe-and-cancel 'xclip -in -selection clipboard'
EOF

# ksqlDB 설치
${local.install_ksqldb}

# 서비스 등록
cat <<EOF | sudo tee /etc/systemd/system/ksqldb.service
${data.template_file.svc_ksqldb.rendered}
EOF

# 서비스 실행
sudo systemctl enable ksqldb
sudo systemctl start ksqldb

sleep 10
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

# ksqlDB 서비스 등록
data "template_file" "svc_ksqldb" {
  template = file("${path.module}/../svc_ksqldb.tpl")
  vars = {
    user = "root",
    timezone = var.timezone
  }
}
