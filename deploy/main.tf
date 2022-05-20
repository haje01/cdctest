provider "aws" {
  region = var.region
}

# SQL Server 보안 그룹
resource "aws_security_group" "sqlserver" {
  name = "${var.name}-sqlserver"

  ingress {
    from_port = 22
    to_port = 22
    description = "From Dev PC to SSH"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 3389
    to_port = 3389
    description = "From Dev PC to Remote Desktop"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 5985
    to_port = 5985
    description = "From Dev PC to WinRM"
    protocol = "tcp"
    cidr_blocks = var.work_cidr
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Debezium to SQL Server"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.debezium.id}"
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
      Name = "${var.name}-sqlserver",
      terraform = "true"
    },
    var.tags
  )
}

# Debezium 보안 그룹
resource "aws_security_group" "debezium" {
  name = "${var.name}-debezium"

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
      Name = "${var.name}-debezium",
      terraform = "true"
    },
    var.tags
  )
}

# SQL Server 인스턴스
resource "aws_instance" "sqlserver" {
  ami = var.sqlserver_ami
  instance_type = var.sqlserver_instance_type
  security_groups = [aws_security_group.sqlserver.name]
  key_name = var.key_pair_name
  get_password_data = true

  connection {
    type = "winrm"
    host = self.public_ip
    user = var.sqlserver_user
    password = var.sqlserver_passwd
    private_key = file(var.private_key_path)
    timeout = "1m"
  }

  # WinRM 설정
  user_data = <<EOF
<powershell>
net user ${var.sqlserver_user} '${var.sqlserver_passwd}' /add /y
net localgroup administrators ${var.sqlserver_user} /add
winrm quickconfig -q
winrm set winrm/config/winrs '@{MaxMemoryPerShellMB="300"}'
winrm set winrm/config '@{MaxTimeoutms="1800000"}'
winrm set winrm/config/service '@{AllowUnencrypted="true"}'
winrm set winrm/config/service/auth '@{Basic="true"}'
netsh advfirewall firewall add rule name="WinRM 5985" protocol=TCP dir=in localport=5985 action=allow
netsh advfirewall firewall add rule name="WinRM 5986" protocol=TCP dir=in localport=5986 action=allow
net stop winrm
sc.exe config winrm start=auto
net start winrm
</powershell>
  EOF

  provisioner "file" {
    source = "init.sql"
    destination = "C:/Windows/Temp/init.sql"
  }

  provisioner "remote-exec" {
    inline = [
      "sqlcmd -i C:\\Windows\\Temp\\init.sql"
    ]
  }

  tags = merge(
    {
      Name = "${var.name}-sqlserver",
      terraform = "true"
    },
    var.tags
  )
}

# Debezium 인스턴스 (Debezium + Kafka + Zookeeper)
resource "aws_instance" "debezium" {
  ami = var.debezium_ami
  instance_type = var.debezium_instance_type
  security_groups = [aws_security_group.debezium.name]
  key_name = var.key_pair_name
  tags = merge(
    {
      Name = "${var.name}-debezium",
      terraform = "true"
    },
    var.tags
  )
}
