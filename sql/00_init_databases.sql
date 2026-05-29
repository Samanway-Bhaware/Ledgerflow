-- 00_init_databases.sql
-- Runs first (alphabetical order) against the warehouse database. Creates a
-- separate database for Airflow's own metadata so the warehouse stays clean.
-- The warehouse db itself and the `pipeline` role come from POSTGRES_DB /
-- POSTGRES_USER in docker-compose.

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
