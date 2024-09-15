CREATE TABLE ensname (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(66) NOT NULL,
    name VARCHAR(1024),
    label_name VARCHAR(1024),
    label VARCHAR(66),
    erc721_token_id VARCHAR(255),
    erc1155_token_id VARCHAR(255),
    parent_node VARCHAR(66),
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    registered_height BIGINT DEFAULT 0,
    registered_hash VARCHAR(66),
    expire_time TIMESTAMP WITHOUT TIME ZONE,
    is_migrated BOOLEAN DEFAULT FALSE,
    is_wrapped BOOLEAN DEFAULT FALSE,
    wrapped_owner VARCHAR(42),
    fuses INT,
    grace_period_ends TIMESTAMP WITHOUT TIME ZONE,
    owner VARCHAR(42),
    resolver VARCHAR(42),
    resolved_address VARCHAR(42),
    reverse_address VARCHAR(42),
    is_primary BOOLEAN DEFAULT FALSE,
    contenthash TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    texts JSONB default '{}'::jsonb,
    resolved_records JSONB default '{}'::jsonb,
    CONSTRAINT unique_ensname UNIQUE (namenode)
);

CREATE INDEX ensname_index ON ensname (name);
CREATE INDEX ensname_label_index ON ensname (label_name);
CREATE INDEX ensname_owner_index ON ensname (owner);
CREATE INDEX ensname_resolved_index ON ensname (resolved_address);
CREATE INDEX ensname_reverse_index ON ensname (reverse_address);


BEGIN;
TRUNCATE TABLE public.ensname;
ALTER SEQUENCE public.ensname_id_seq RESTART WITH 1;
COMMIT;


CREATE TABLE ens_resolved_records (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(66) NOT NULL,
    resolved_records JSONB default '{}'::jsonb,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_ens_resolved_records UNIQUE (namenode)
);