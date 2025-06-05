/*
  Create database 'DataWarehouse' and schemas 'bronze', 'silver' and 'gold' (also dbs in mysql)
WARNING:
  Drops databases and schemas if exists, permanently deleting all the data.
  Ensure having all the backups before running the script.
*/


-- Удаляем, если существует
DROP DATABASE IF EXISTS DataWarehouse;
DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gold;

-- Создаем новую базу
CREATE DATABASE DataWarehouse;

-- Выбираем базу для работы
USE DataWarehouse;


-- Create schemas
create schema bronze;
create schema silver;
create schema gold;
