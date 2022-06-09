CREATE DATABASE test;

USE test;

CREATE USER '${user}'@'%' IDENTIFIED BY '${passwd}';
GRANT ALL PRIVILEGES ON test.* TO '${user}'@'%';
