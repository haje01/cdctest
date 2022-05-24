CREATE LOGIN ${user} WITH PASSWORD = '${passwd}', check_policy = off
GO

CREATE DATABASE test
GO

USE test
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N'${user}')
BEGIN
    CREATE USER ${user} FOR LOGIN ${user}
    EXEC sp_addrolemember N'db_owner', N'${user}'
END;
GO
