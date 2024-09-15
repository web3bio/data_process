CREATE TABLE lensv2_profile (
    profile_id BIGINT NOT NULL,
    profile_id_hex VARCHAR(255) NOT NULL,
    name VARCHAR(1024),
    handle_name VARCHAR(1024),
    namespace VARCHAR(255),
    label_name VARCHAR(1024),
    is_primary BOOLEAN DEFAULT FALSE,
    handle_node_id VARCHAR(255),
    handle_token_id VARCHAR(255),
    avatar TEXT,
    display_name TEXT,
    description TEXT,
    cover_picture TEXT,
    tx_hash VARCHAR(255),
    network VARCHAR(66),
    address VARCHAR(66),
    texts JSONB default '{}'::jsonb, -- profile_config/attribute
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    create_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_lensv2_profile UNIQUE (profile_id)
);

CREATE INDEX idx_lensv2_profile_handle_name ON lensv2_profile (handle_name);
CREATE INDEX idx_lensv2_profile_label_name ON lensv2_profile (label_name);
CREATE INDEX idx_lensv2_profile_address ON lensv2_profile (address);

CREATE TABLE lensv2_social (
    profile_id BIGINT NOT NULL,
    is_power BOOLEAN DEFAULT FALSE,
    follower INT DEFAULT 0,
    following INT DEFAULT 0,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (profile_id)
);

CREATE INDEX idx_lensv2_social_follower ON lensv2_social (follower);

BEGIN;
TRUNCATE TABLE public.lensv2_social;
COMMIT;