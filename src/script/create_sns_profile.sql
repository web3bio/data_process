CREATE TABLE sns_profile (
    id SERIAL PRIMARY KEY,
    namenode VARCHAR(66) NOT NULL,
    name VARCHAR(1024),
    label_name VARCHAR(1024),
    parent_node VARCHAR(66),
    is_tokenized BOOLEAN DEFAULT FALSE,
    nft_owner VARCHAR(66),
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    expire_time TIMESTAMP WITHOUT TIME ZONE,
    owner VARCHAR(66),
    resolver VARCHAR(66),
    resolved_address VARCHAR(66),
    reverse_address VARCHAR(66),
    is_primary BOOLEAN DEFAULT FALSE,
    contenthash TEXT,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    texts JSONB default '{}'::jsonb,
    resolved_records JSONB default '{}'::jsonb,
    CONSTRAINT unique_sns_profile UNIQUE (namenode)
);

CREATE INDEX sns_profile_name_index ON sns_profile (name);
CREATE INDEX sns_profile_label_name_index ON sns_profile (label_name);
CREATE INDEX sns_profile_nft_owner ON sns_profile (nft_owner);
CREATE INDEX sns_profile_owner_index ON sns_profile (owner);
CREATE INDEX sns_profile_resolved_index ON sns_profile (resolved_address);
CREATE INDEX sns_profile_reverse_index ON sns_profile (reverse_address);
