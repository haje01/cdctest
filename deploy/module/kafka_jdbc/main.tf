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
chmod 775 ${basename(var.kafka_jdbc_connector)}
rm ${basename(var.kafka_jdbc_connector)}
sed -i "s/#plugin.path=/plugin.path=\\/home\\/ubuntu\\/$kafka_dir\\/connectors/" ../config/connect-distributed.properties
cd ..
EOT
  install_kafka_s3_sink = <<EOT
# Kafka S3 Sink Connector 설치
mkdir -p connectors
mv /tmp/${basename(var.kafka_s3_sink)} connectors/
cd connectors/
unzip ${basename(var.kafka_s3_sink)}
rm ${basename(var.kafka_s3_sink)}
cd ..
EOT
  install_mysql_jdbc_driver = <<EOT
sudo apt install -y /tmp/${basename(var.mysql_jdbc_driver)}
cp /usr/share/java/mysql-connector-java-*.jar /home/ubuntu/$kafka_dir/connectors/$kjc_dir/lib
EOT
  install_mysql_dbzm_connector = <<EOT
mkdir -p connectors
mv /tmp/${basename(var.mysql_dbzm_connector)} connectors/
cd connectors/
tar xzvf ${basename(var.mysql_dbzm_connector)}
rm ${basename(var.mysql_dbzm_connector)}
# sed -i "s/#plugin.path=/plugin.path=\\/home\\/ubuntu\\/$kafka_dir\\/connectors/" ../config/connect-distributed.properties
cd ..
EOT
  install_mssql_dbzm_connector = <<EOT
mkdir -p connectors
mv /tmp/${basename(var.mssql_dbzm_connector)} connectors/
cd connectors/
tar xzvf ${basename(var.mssql_dbzm_connector)}
rm ${basename(var.mssql_dbzm_connector)}
# sed -i "s/#plugin.path=/plugin.path=\\/home\\/ubuntu\\/$kafka_dir\\/connectors/" ../config/connect-distributed.properties
cd ..
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
    source = var.kafka_jdbc_connector
    destination = "/tmp/${basename(var.kafka_jdbc_connector)}"
  }

  provisioner "file" {
    source = var.kafka_s3_sink
    destination = "/tmp/${basename(var.kafka_s3_sink)}"
  }

  # MySQL JDBC Driver 복사
  provisioner "file" {
    source = var.mysql_jdbc_driver
    destination = "/tmp/${basename(var.mysql_jdbc_driver)}"
  }

  provisioner "file" {
    source = var.mysql_dbzm_connector
    destination = "/tmp/${basename(var.mysql_dbzm_connector)}"
  }

  provisioner "file" {
    source = var.mssql_dbzm_connector
    destination = "/tmp/${basename(var.mssql_dbzm_connector)}"
  }

  provisioner "remote-exec" {
    inline = [<<EOT
cloud-init status --wait
kafka_file=${basename(var.kafka_url)}
kafka_dir=$(basename $kafka_file .tgz)
kjc_file=${basename(var.kafka_jdbc_connector)}
kjc_dir=$(basename $kjc_file .zip)

sudo apt update
sudo sed -i 's/#$nrconf{restart} = '"'"'i'"'"';/$nrconf{restart} = '"'"'a'"'"';/g' /etc/needrestart/needrestart.conf
sudo sed -i 's/#MaxSessions 10/MaxSessions 200/g' /etc/ssh/sshd_config
sudo service sshd restart
sudo apt install -y unzip jq kafkacat
# Kafka 설치
sudo apt install -y openjdk-8-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.kenv
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.kenv
echo "export KAFKA_HOME=~/$kafka_dir" >> ~/.kenv
echo "alias kcat=kafkacat" >> ~/.kenv
wget -nv ${var.kafka_url}
tar xzf $kafka_file
rm $kafka_file

## 설정
cd $kafka_dir
sed -i "s/#listeners=PLAINTEXT:\\/\\/:9092/listeners=INTERNAL:\\/\\/0.0.0.0:9092,EXTERNAL:\\/\\/0.0.0.0:19092/" config/server.properties
sed -i "s/#advertised.listeners=PLAINTEXT:\\/\\/your.host.name:9092/advertised.listeners=INTERNAL:\\/\\/${aws_instance.kafka.private_ip}:9092,EXTERNAL:\\/\\/${aws_eip.kafka.public_ip}:19092/" config/server.properties
# Connector 로그 뒤의 로그 출처 제거
sed -i "s/connect.log.pattern=\\[%d\\] %p %X{connector.context}%m (%c:%L)%n/connect.log.pattern=[%d] %p %X{connector.context}%m%n/" config/connect-log4j.properties
echo "listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT" >> config/server.properties
echo "inter.broker.listener.name=INTERNAL" >> config/server.properties
echo "auto.create.topics.enable=false" >> config/server.properties

sudo mkdir -p /data/zookeeper
sudo mkdir -p /data/kafka
sudo chown ubuntu:ubuntu -R /data
sed -i "s/dataDir=\\/tmp\\/zookeeper/dataDir=\\/data\\/zookeeper/" config/zookeeper.properties
sed -i "s/log.dirs=\\/tmp\\/kafka-logs/log.dirs=\\/data\\/kafka/" config/server.properties
echo "export PATH=$PATH:~/$kafka_dir/bin" >> ~/.kenv
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
${local.install_kafka_jdbc_connector}
${local.install_kafka_s3_sink}
${local.install_mysql_jdbc_driver}
${local.install_mysql_dbzm_connector}
${local.install_mssql_dbzm_connector}
# MSSQL JDBC Driver 는 confluentinc-kafka-connect-jdbc 에 이미 포함

# 서비스 등록
# 참고: https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-20-04#step-6-mdash-hardening-the-kafka-server
#
cat <<EOF | sudo tee /etc/systemd/system/zookeeper.service
${data.template_file.svc_zookeeper.rendered}
EOF
cat <<EOF | sudo tee /etc/systemd/system/kafka.service
${data.template_file.svc_kafka.rendered}
EOF
cat <<EOF | sudo tee /etc/systemd/system/kafka-connect.service
${data.template_file.svc_connect.rendered}
EOF
sudo systemctl enable zookeeper
sudo systemctl enable kafka
sudo systemctl enable kafka-connect

# 실행
sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl start kafka-connect

# Connector 등록
# "bash /tmp/regretry.sh
sleep 10
EOT
    ]
  }
}


# Zookeeper 서비스 등록
data "template_file" "svc_zookeeper" {
  template = file("${path.module}/../svc_zookeeper.tpl")
  vars = {
    user = "ubuntu",
    kafka_dir = trimsuffix(basename(var.kafka_url), ".tgz")
  }
}

# Kafka 서비스 등록
data "template_file" "svc_kafka" {
  template = file("${path.module}/../svc_kafka.tpl")
  vars = {
    user = "ubuntu",
    kafka_dir = trimsuffix(basename(var.kafka_url), ".tgz")
    timezone = var.timezone
  }
}

# Kafka Connect 서비스 등록
data "template_file" "svc_connect" {
  template = file("${path.module}/../svc_connect.tpl")
  vars = {
    user = "ubuntu",
    kafka_dir = trimsuffix(basename(var.kafka_url), ".tgz")
    timezone = var.timezone
  }
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
