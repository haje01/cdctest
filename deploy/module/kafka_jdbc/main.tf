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
    description = "From Consumer to Kafka"
    protocol = "tcp"
    security_groups = [
      "${var.consumer_sg_id}"
    ]
  }

  ingress {
    from_port = 8083
    to_port = 8083
    description = "From Consumer to Kafka Connect"
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

locals {
  install_kafka_jdbc_connector = <<EOT
# Kafka JDBC Connector 설치
mkdir -p connectors
mv /tmp/${basename(var.kafka_jdbc_connector)} connectors/
cd connectors/
unzip ${basename(var.kafka_jdbc_connector)}
rm ${basename(var.kafka_jdbc_connector)}
kjc_file=${basename(var.kafka_jdbc_connector)}
kjc_dir=$(basename $kjc_file .zip)
sed -i "s/#plugin.path=/plugin.path=\\/home\\/ubuntu\\/$kafka_dir\\/connectors/" ../config/connect-distributed.properties
cd ..
EOT
  run_kafka_jdbc_connector = <<EOT
bin/connect-distributed.sh -daemon config/connect-distributed.properties
sleep 10
# screen -S kafka-connect -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/connect-distributed.sh config/connect-distributed.properties; exec bash'
# while ! curl -s localhost:8083/connectors >> /dev/null 2>&1 ; do sleep 5 ; done
EOT
  install_mysql_jdbc_driver = <<EOT
sudo apt install -y /tmp/${basename(var.mysql_jdbc_driver)}
cp /usr/share/java/mysql-connector-java-*.jar ~/$kafka_dir/connectors/$kjc_dir/lib
EOT
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

  # Kafka Connector 복사
  provisioner "file" {
    source = var.kafka_jdbc_connector
    destination = "/tmp/${basename(var.kafka_jdbc_connector)}"
  }

  # MySQL JDBC Driver 복사
  provisioner "file" {
    source = var.mysql_jdbc_driver
    destination = "/tmp/${basename(var.mysql_jdbc_driver)}"
  }

  provisioner "remote-exec" {
    inline = [<<EOT
cloud-init status --wait
sudo apt update
sudo apt install -y unzip

# Kafka 설치
sudo apt install -y openjdk-8-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.myenv
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.myenv
wget -nv ${var.kafka_url}
kafka_file=${basename(var.kafka_url)}
kafka_dir=$(basename $kafka_file .tgz)
echo "export KAFKA_HOME=~/$kafka_dir" >> ~/.myenv
tar xzf $kafka_file
rm $kafka_file
cd $kafka_dir
sed -i "s/#listeners=PLAINTEXT:\\/\\/:9092/listeners=INTERNAL:\\/\\/0.0.0.0:9092,EXTERNAL:\\/\\/0.0.0.0:19092/" config/server.properties
sed -i "s/#advertised.listeners=PLAINTEXT:\\/\\/your.host.name:9092/advertised.listeners=INTERNAL:\\/\\/${self.private_ip}:9092,EXTERNAL:\\/\\/${self.public_ip}:19092/" config/server.properties
echo "listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT" >> config/server.properties
echo "inter.broker.listener.name=INTERNAL" >> config/server.properties

echo "export PATH=$PATH:~/$kafka_dir/bin" >> ~/.myenv
cat ~/.myenv >> ~/.bashrc

# 플러그인 설치
${var.kafka_jdbc_connector != "" ? local.install_kafka_jdbc_connector : ""}
${var.mysql_jdbc_driver != ""? local.install_mysql_jdbc_driver : ""}
# MSSQL JDBC Driver 는 confluentinc-kafka-connect-jdbc 에 이미 포함

# 실행
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties
${var.kafka_jdbc_connector != "" ? local.run_kafka_jdbc_connector : ""}

# screen -S zookeeper -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash'
# screen -S kafka -dm bash -c 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; sudo bin/kafka-server-start.sh config/server.properties; exec bash'

# Connector 등록
# "bash /tmp/regretry.sh
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


# Kafka JDBC Connector 등록
# data "template_file" "regconn" {
#   template = file("${path.module}/regconn.tpl")
#   vars = {
#     host = aws_instance.mysql.private_ip,
#     user = var.db_user,
#     passwd = random_string.db_passwd.result,
#     port = var.db_port
#   }
# }
