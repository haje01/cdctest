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
    from_port = 19092
    to_port = 19092
    description = "From Dev PC to Kafka"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 9092
    to_port = 9092
    description = "From Producer to Kafka"
    protocol = "tcp"
    security_groups = [
      "${var.producer_sg_id}"
    ]
  }

  ingress {
    from_port = 9092
    to_port = 9092
    description = "From Consumer to Kafka"
    protocol = "tcp"
    security_groups = [
      "${var.consumer_sg_id}"
    ]
  }

  ingress {
    from_port = 9092
    to_port = 9092
    description = "From ksqlDB to Kafka"
    protocol = "tcp"
    security_groups = [
      "${var.ksqldb_sg_id}"
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

locals {
  install_kafka_s3_sink = <<EOT
# Kafka S3 Sink Connector 설치
sudo mv /tmp/${basename(var.kafka_s3_sink)} /usr/share/java
cd /usr/share/java
sudo unzip ${basename(var.kafka_s3_sink)}
sudo rm ${basename(var.kafka_s3_sink)}
cd -
EOT
}

resource "aws_eip" "kafka" {
  instance = aws_instance.kafka.id
  vpc = true
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

  root_block_device {
    # encrypted = true  # Hibernate 테스트용
    volume_size = 100
  }

  # hibernation = true

  tags = merge(
    {
      Name = "${var.name}-kafka",
      terraform = "true"
    },
    var.tags
  )
}

resource "null_resource" "kafka_public_ip" {
  triggers = {
    instance = aws_instance.kafka.id
  }
  connection {
    host = aws_eip.kafka.public_ip
    user = "ubuntu"
    private_key = file(var.private_key)
    agent = false
  }

  provisioner "file" {
    source = var.kafka_s3_sink
    destination = "/tmp/${basename(var.kafka_s3_sink)}"
  }

  provisioner "remote-exec" {
    inline = [<<EOT
cloud-init status --wait
sudo apt-get update
sudo sed -i 's/#$nrconf{restart} = '"'"'i'"'"';/$nrconf{restart} = '"'"'a'"'"';/g' /etc/needrestart/needrestart.conf

# Confluent Community Edition 설치
wget -qO - https://packages.confluent.io/deb/7.2/archive.key | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/7.2 stable main"
# 현재(2022-11-02) 공식 지원 OS 는 20.x 대 (focal)
sudo add-apt-repository -y "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main"
sudo apt-get install -y confluent-community-2.13

sudo apt install -y unzip jq kafkacat

sudo apt install -y openjdk-11-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.kenv
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.kenv
echo "alias kcat=kafkacat" >> ~/.kenv

# 설정
sudo sed -i "s/#listeners=PLAINTEXT:\\/\\/:9092/listeners=INTERNAL:\\/\\/0.0.0.0:9092,EXTERNAL:\\/\\/0.0.0.0:19092/" /etc/kafka/server.properties
sudo sed -i "s/#advertised.listeners=PLAINTEXT:\\/\\/your.host.name:9092/advertised.listeners=INTERNAL:\\/\\/${aws_instance.kafka.private_ip}:9092,EXTERNAL:\\/\\/${aws_eip.kafka.public_ip}:19092/" /etc/kafka/server.properties
sudo sed -i "s/#listener.security.protocol.map=.*/listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT/" /etc/kafka/server.properties
echo 'inter.broker.listener.name=INTERNAL' | sudo tee -a /etc/kafka/server.properties
echo "auto.create.topics.enable=false" | sudo tee -a /etc/kafka/server.properties

cat ~/.kenv >> ~/.bashrc

sudo cp -p /usr/share/zoneinfo/Asia/Seoul /etc/localtime
export TZ="Asia/Seoul"

cat <<EOF > ~/.tmux.conf
set -g mouse on
set-option -g status-right ""
set-option -g history-limit 10000
set-window-option -g mode-keys vi
bind-key -T copy-mode-vi v send -X begin-selection
bind-key -T copy-mode-vi V send -X select-line
bind-key -T copy-mode-vi y send -X copy-pipe-and-cancel 'xclip -in -selection clipboard'
EOF

# 플러그인 설치
${local.install_kafka_s3_sink}

# 실행
sudo systemctl start confluent-zookeeper
sudo systemctl start confluent-kafka
sudo systemctl start confluent-kafka-connect

sleep 10
EOT
    ]
  }
}
