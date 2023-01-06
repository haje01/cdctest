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

  ingress {
    from_port = 8081
    to_port = 8081
    description = "From Dev PC to Schema Registry"
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
sudo apt-get update
sudo sed -i 's/#$nrconf{restart} = '"'"'i'"'"';/$nrconf{restart} = '"'"'a'"'"';/g' /etc/needrestart/needrestart.conf

# Confluent Community Edition 설치
wget -qO - https://packages.confluent.io/deb/7.2/archive.key | sudo apt-key add -
sudo add-apt-repository -y "deb [arch=amd64] https://packages.confluent.io/deb/7.2 stable main"
# 현재(2022-11-02) 공식 지원 OS 가 20.x 대 (focal)
sudo add-apt-repository -y "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main"
sudo apt-get install -y confluent-community-2.13

sudo apt install -y unzip jq

# ksqlDB 설정
sudo sed -i 's/bootstrap.servers=localhost:9092/bootstrap.servers=${var.kafka_private_ip}:9092/' /etc/ksqldb/ksql-server.properties
# 토픽 처음부터
sudo echo 'ksql.streams.auto.offset.reset=earliest' >> /etc/ksqldb/ksql-server.properties
# Timeout 에러 방지
echo 'ksql.streams.shutdown.timeout.ms=30000' | sudo tee -a /etc/ksqldb/ksql-server.properties
echo 'ksql.idle.connection.timeout.seconds=30000' | sudo tee -a /etc/ksqldb/ksql-server.properties

# schema registry 설정
sudo sed -i 's/kafkastore.bootstrap.servers=.*/kafkastore.bootstrap.servers=PLAINTEXT:\/\/${var.kafka_private_ip}:9092/' /etc/schema-registry/schema-registry.properties

sudo apt install -y openjdk-11-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
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


# 서비스 실행
sudo systemctl start confluent-ksqldb
sudo systemctl start confluent-schema-registry

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
