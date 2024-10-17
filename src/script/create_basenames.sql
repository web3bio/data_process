CREATE TABLE basenames_txlogs (
    id SERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITHOUT TIME ZONE,
    transaction_hash VARCHAR(66) NOT NULL,
    transaction_index INT,
    log_index INT,
    contract_address VARCHAR(42),
    contract_label VARCHAR(66),
    method_id VARCHAR(66),
    signature TEXT,
    decoded TEXT,
    CONSTRAINT unique_basenames_txlogs UNIQUE (transaction_hash, transaction_index, log_index)
);

CREATE INDEX basenames_txlogs_timestamp_index ON basenames_txlogs (block_timestamp);

BEGIN;
TRUNCATE TABLE public.basenames_txlogs;
ALTER SEQUENCE public.basenames_txlogs_id_seq RESTART WITH 1;
COMMIT;


CREATE TABLE basenames (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(66) NOT NULL,
    name VARCHAR(1024),
    label_name VARCHAR(1024),
    label VARCHAR(66),
    erc721_token_id VARCHAR(255),
    parent_node VARCHAR(66),
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    registered_height BIGINT DEFAULT 0,
    registered_hash VARCHAR(66),
    expire_time TIMESTAMP WITHOUT TIME ZONE,
    grace_period_ends TIMESTAMP WITHOUT TIME ZONE,
    owner VARCHAR(42),
    resolver VARCHAR(42),
    resolved_address VARCHAR(42),
    reverse_address VARCHAR(42),
    is_primary BOOLEAN DEFAULT FALSE,
    contenthash TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE,
    texts JSONB default '{}'::jsonb,
    resolved_records JSONB default '{}'::jsonb,
    CONSTRAINT unique_basenames UNIQUE (namenode)
);

CREATE INDEX basenames_name_index ON basenames (name);
CREATE INDEX basenames_label_name_index ON basenames (label_name);
CREATE INDEX basenames_owner_index ON basenames (owner);
CREATE INDEX basenames_resolved_index ON basenames (resolved_address);
CREATE INDEX basenames_reverse_index ON basenames (reverse_address);


CREATE TABLE basenames_record (
    id SERIAL PRIMARY KEY,
    block_timestamp TIMESTAMP WITHOUT TIME ZONE,
    namenode VARCHAR(66) NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    log_count INT NOT NULL,
    is_registered BOOLEAN DEFAULT FALSE,
    update_record TEXT,
    CONSTRAINT unique_basenames_record UNIQUE (namenode, transaction_hash)
);

CREATE INDEX timestamp_basenames_record_index ON basenames_record (block_timestamp);

BEGIN;
TRUNCATE TABLE public.basenames_record;
ALTER SEQUENCE public.basenames_record_id_seq RESTART WITH 1;
COMMIT;