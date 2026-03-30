/*
This script creates the 'DATAWAREHOUSE' database and three schemas (gold, silver and bronze schema)

Warning!
this script will drop the entire 'DATAWAREHOUSE' database if it already exists and all data will be deleted permanently.
Proceed with caution 
*/


USE master;
GO

IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
	ALTER DATABASE DATAWAREHOUSE SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DATAWAREHOUSE;
END;
GO

CREATE DATABASE DATAWAREHOUSE
GO

USE DATAWAREHOUSE;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
