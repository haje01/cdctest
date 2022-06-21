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

resource "aws_eip" "kafka" {
  instance = aws_instance.kafka
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
kafka_file=${basename(var.kafka_url)}
kafka_dir=$(basename $kafka_file .tgz)

sudo apt update
sudo apt install -y unzip

# Kafka 설치
sudo apt install -y openjdk-8-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.kenv
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.kenv
echo "export KAFKA_HOME=~/$kafka_dir" >> ~/.kenv
wget -nv ${var.kafka_url}
tar xzf $kafka_file
rm $kafka_file

# 설정
cd $kafka_dir
sed -i "s/#listeners=PLAINTEXT:\\/\\/:9092/listeners=INTERNAL:\\/\\/0.0.0.0:9092,EXTERNAL:\\/\\/0.0.0.0:19092/" config/server.properties
sed -i "s/#advertised.listeners=PLAINTEXT:\\/\\/your.host.name:9092/advertised.listeners=INTERNAL:\\/\\/${self.private_ip}:9092,EXTERNAL:\\/\\/${self.public_ip}:19092/" config/server.properties
echo "listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT" >> config/server.properties
echo "inter.broker.listener.name=INTERNAL" >> config/server.properties

sudo mkdir -p /data/zookeeper
sudo mkdir -p /data/kafka
sudo chown ubuntu:ubuntu -R /data
sed -i "s/dataDir=\\/tmp\\/zookeeper/dataDir=\\/data\\/zookeeper/" config/zookeeper.properties
sed -i "s/log.dirs=\\/tmp\\/kafka-logs/log.dirs=\\/data\\/kafka/" config/server.properties
echo "export PATH=$PATH:~/$kafka_dir/bin" >> ~/.kenv
cat ~/.kenv >> ~/.bashrc

# 실행
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties
sleep 10
EOF
EOT
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
