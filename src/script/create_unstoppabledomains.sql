CREATE TABLE unstoppabledomains (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(255) NOT NULL,
    name VARCHAR(1024),
    label_name VARCHAR(1024),
    label VARCHAR(1024),
    erc721_token_id VARCHAR(255),
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    registered_height BIGINT DEFAULT 0,
    registered_hash VARCHAR(66),
    registry VARCHAR(66),
    expire_time TIMESTAMP WITHOUT TIME ZONE,
    grace_period_ends TIMESTAMP WITHOUT TIME ZONE,
    owner VARCHAR(66),
    resolver VARCHAR(66),
    resolved_address VARCHAR(66),
    reverse_address VARCHAR(66),
    is_primary BOOLEAN DEFAULT FALSE,
    contenthash TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE,
    texts JSONB default '{}'::jsonb,
    resolved_records JSONB default '{}'::jsonb,
    CONSTRAINT unique_unstoppabledomains UNIQUE (namenode)
);

CREATE INDEX unstoppabledomains_name_index ON unstoppabledomains (name);
CREATE INDEX unstoppabledomains_label_name_index ON unstoppabledomains (label_name);
CREATE INDEX unstoppabledomains_owner_index ON unstoppabledomains (owner);
CREATE INDEX unstoppabledomains_resolved_index ON unstoppabledomains (resolved_address);
CREATE INDEX unstoppabledomains_reverse_index ON unstoppabledomains (reverse_address);