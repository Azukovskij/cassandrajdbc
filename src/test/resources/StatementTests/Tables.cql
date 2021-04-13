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
    
    
    
CREATE TABLE Joined (
    id uuid,
    "value" varchar,
    simple_fk uuid,
    PRIMARY KEY(id));
    
   
CREATE TABLE Joined2 (
    id uuid,
    "value" varchar,
    simple_fk uuid,
    PRIMARY KEY(id));
    
    
CREATE CUSTOM INDEX SimpleValueIdx ON Simple ("value") USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS', 'case_sensitive': 'false'};
CREATE CUSTOM INDEX JoinedValueIdx ON Joined ("value") USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS', 'case_sensitive': 'false'};
    