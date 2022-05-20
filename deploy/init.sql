CREATE LOGIN tester WITH PASSWORD = 'tester@381', check_policy = off
GO

CREATE DATABASE test
USE test
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N'tester')
BEGIN
    CREATE USER tester FOR LOGIN tester
    EXEC sp_addrolemember N'db_owner', N'tester'
END;
GO
