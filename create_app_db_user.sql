\set ON_ERROR_STOP on

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'app_user') THEN
    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_user', :'app_password');
  ELSE
    EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'app_user', :'app_password');
  END IF;
END $$;

SELECT format('CREATE DATABASE %I OWNER %I', :'app_db', :'app_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'app_db')\gexec

SELECT format('ALTER DATABASE %I OWNER TO %I', :'app_db', :'app_user')\gexec

\connect :app_db

SELECT format('ALTER SCHEMA public OWNER TO %I', :'app_user')\gexec
SELECT format('GRANT ALL PRIVILEGES ON SCHEMA public TO %I', :'app_user')\gexec
