/* 
===============================================================
Create Database and Schemas
===============================================================

Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists. If it exists, it is dropped and recreated. After that, it creates 3 schemas within the database: 'bronze', 'silver' and 'gold'.

Warning:
  Running this will drop the entire 'DataWarehouse' database if it already exists. 
  All the data will be permanently deleted. 
  Proceed with caution!
*/ 

USE master;
GO

-- Drop and create the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO


CREATE DATABASE DataWarehouse;

-- Switch to DataWarehouse database
USE DataWarehouse;
GO
  
-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO 
CREATE SCHEMA gold;
GO
