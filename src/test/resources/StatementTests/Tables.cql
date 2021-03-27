CREATE TABLE AllTypes (
    id uuid,
    "varchar" varchar,
    "text" text,
    "int" int,
    "bigint" bigint,
    "varint" varint,
    "smallint" smallint,
    "date" date,
    "timestamp" timestamp,
    "time" time,
    "float" float,
    "decimal" decimal,
    "double" double,
    "boolean" boolean,
    "blob" blob,
    PRIMARY KEY(id));


CREATE TABLE Simple (
    id uuid,
    "value" varchar,
    PRIMARY KEY(id));