-- scripts/init_db.sql
-- Runs once when the Docker container is first created.
-- Installs the AGE extension so it's ready before our Python script runs.

CREATE EXTENSION IF NOT EXISTS age;
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
