CREATE DATABASE test;

USE test;

CREATE USER '${user}'@'localhost' IDENTIFIED BY '${passwd}';
GRANT ALL PRIVILEGES ON test.* TO '${user}'@'localhost';
