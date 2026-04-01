\echo 'OpenAlgo PostgreSQL bootstrap'
\echo 'Target profile: ENV=local'
\echo 'Host: localhost'
\echo 'Port: 5433'
\echo 'User: postgres'
\echo 'This script creates the databases required by OpenAlgo.'

\set ON_ERROR_STOP on

\connect postgres postgres localhost 5433

SELECT format('CREATE DATABASE %I OWNER %I', 'openalgo', 'postgres')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'openalgo')
\gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', 'openalgo', 'postgres')
\gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', 'openalgo', 'postgres')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'openalgo_logs', 'postgres')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'openalgo_logs')
\gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', 'openalgo_logs', 'postgres')
\gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', 'openalgo_logs', 'postgres')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'openalgo_latency', 'postgres')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'openalgo_latency')
\gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', 'openalgo_latency', 'postgres')
\gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', 'openalgo_latency', 'postgres')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'openalgo_sandbox', 'postgres')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'openalgo_sandbox')
\gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', 'openalgo_sandbox', 'postgres')
\gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', 'openalgo_sandbox', 'postgres')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'openalgo_health', 'postgres')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'openalgo_health')
\gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', 'openalgo_health', 'postgres')
\gexec
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', 'openalgo_health', 'postgres')
\gexec

\echo 'Bootstrap complete.'
