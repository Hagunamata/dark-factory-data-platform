-- =============================================================================
-- Dark Factory Data Platform — Postgres database initialisation
-- =============================================================================
-- Runs before 01_schemas.sql (alphabetical order in /docker-entrypoint-initdb.d).
-- Creates a dedicated `airflow` database so the Airflow metadata DB does not
-- collide with the `raw` and `analytics` schemas in the main `darkfactory` DB.
-- =============================================================================

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
