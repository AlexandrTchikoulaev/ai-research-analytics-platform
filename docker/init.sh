#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE gestao_db;
    CREATE DATABASE vector_db;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "vector_db" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS vector;
EOSQL
