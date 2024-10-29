-- Create the user if it doesn't exist (optional if already done in your setup)
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'caio') THEN
      CREATE ROLE caio LOGIN PASSWORD 'trakinas';
   END IF;
END
$$;

-- Grant privileges to the user
ALTER ROLE caio WITH SUPERUSER;
ALTER ROLE caio CREATEDB;

-- Create the database (optional if `sales_data` is already created via docker-compose)
CREATE DATABASE sales_data OWNER caio;

-- Connect to the new database
\connect sales_data


-- Grant all privileges on the sales_data database to user caio
GRANT ALL PRIVILEGES ON DATABASE sales_data TO caio;
