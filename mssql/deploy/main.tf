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
    description = "From Inserter to SQL Server"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.inserter.id}"
    ]
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Selector to SQL Server"
    protocol = "tcp"
    security_groups = [
      "${aws_security_group.selector.id}"
    ]
  }

  ingress {
    from_port = 1433
    to_port = 1433
    description = "From Dev PC to SQL Server"
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
      Name = "${var.name}-sqlserver",
      terraform = "true"
    },
    var.tags
  )
}


data "template_file" "initdb" {
  template = file("${path.module}/init.tpl")
  vars = {
    user = var.db_user,
    passwd = var.db_passwd
  }
}

# SQL Server 인스턴스
resource "aws_instance" "sqlserver" {
  ami = var.sqlserver_ami
  instance_type = var.sqlserver_instance_type
  security_groups = [aws_security_group.sqlserver.name]
  key_name = var.key_pair_name
  get_password_data = true

  user_data_replace_on_change = true
  user_data = <<EOF
<powershell>
# WinRM 설정
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

# 로그인 인증을 Mixed 로 변경
Set-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\MSSQL14.MSSQLSERVER\\MSSQLServer' -Name LoginMode -Value 2
Restart-Service -Force MSSQLSERVER

# Chocolatey 설치
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))
</powershell>
  EOF

  connection {
    type = "winrm"
    host = self.public_ip
    user = var.sqlserver_user
    password = var.sqlserver_passwd
    timeout = "1m"
  }

  provisioner "file" {
    content = data.template_file.initdb.rendered
    destination = "C:/Windows/Temp/init.sql"
  }

  provisioner "remote-exec" {
    inline = [
      # DB 및 유저 초기화
      "sqlcmd -i C:\\Windows\\Temp\\init.sql",
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

# Inserter 보안 그룹
resource "aws_security_group" "inserter" {
  name = "${var.name}-inserter"

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
      Name = "${var.name}-inserter",
      terraform = "true"
    },
    var.tags
  )
}

# Inserter 인스턴스
resource "aws_instance" "inserter" {
  ami = var.insel_ami
  instance_type = var.insel_instance_type
  security_groups = [aws_security_group.inserter.name]
  key_name = var.key_pair_name

  user_data_replace_on_change = true
  user_data = <<EOF
#!/bin/bash
sudo apt update
sudo apt install -y python3-pip
cd /home/ubuntu
git clone https://github.com/haje01/dbztest.git
cd dbztest && pip3 install -r requirements.txt
  EOF

  tags = merge(
    {
      Name = "${var.name}-inserter",
      terraform = "true"
    },
    var.tags
  )
}

# Selector 보안 그룹
resource "aws_security_group" "selector" {
  name = "${var.name}-selector"

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
      Name = "${var.name}-selector",
      terraform = "true"
    },
    var.tags
  )
}

# Inserter 인스턴스
resource "aws_instance" "selector" {
  ami = var.insel_ami
  instance_type = var.insel_instance_type
  security_groups = [aws_security_group.selector.name]
  key_name = var.key_pair_name

  user_data_replace_on_change = true
  user_data = <<EOF
#!/bin/bash
sudo apt update
sudo apt install -y python3-pip
cd /home/ubuntu
git clone https://github.com/haje01/dbztest.git
cd dbztest && pip3 install -r requirements.txt
  EOF

  tags = merge(
    {
      Name = "${var.name}-selector",
      terraform = "true"
    },
    var.tags
  )
}
