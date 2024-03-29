ALTER SYSTEM SET wal_level = logical;

CREATE SCHEMA postgres;

CREATE TABLE postgres.assinaturas (
    "ID" integer PRIMARY KEY,
    "ID CLIENTE" integer,
    "valor" float,
    "plano pro ?" varchar
);
CREATE TABLE postgres.clientes (
    ID integer PRIMARY KEY,
    NOME varchar
);


ALTER TABLE "assinaturas" REPLICA IDENTITY FULL;
ALTER TABLE "clientes" REPLICA IDENTITY FULL;

CREATE USER debezium PASSWORD 'password';
ALTER USER debezium REPLICATION LOGIN;
