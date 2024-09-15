CREATE TABLE farcaster_profile (
    id SERIAL PRIMARY KEY,
    fid BIGINT NOT NULL,
    fname VARCHAR(1024),
    label_name VARCHAR(1024),
    alias JSONB DEFAULT '[]'::jsonb,
    avatar TEXT, -- 1
    display_name TEXT, -- 2
    description TEXT, -- 3
    cover_picture TEXT,
    custody_address VARCHAR(66) NOT NULL,
    network VARCHAR(66),
    address VARCHAR(66), -- first signer address
    texts JSONB DEFAULT '{}'::jsonb, -- location
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    create_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT unique_farcaster_profile UNIQUE (fid, fname)
);

CREATE INDEX idx_fc_profile_custody_address ON farcaster_profile (custody_address);
CREATE INDEX idx_fc_profile_fid ON farcaster_profile (fid);
CREATE INDEX idx_fc_profile_fname ON farcaster_profile (fname);
CREATE INDEX idx_fc_profile_label_name ON farcaster_profile (label_name);
CREATE INDEX idx_fc_profile_address ON farcaster_profile (address);

CREATE TABLE farcaster_verified_address (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fid BIGINT NOT NULL,
    network VARCHAR(66) NOT NULL,
    address VARCHAR(66) NOT NULL,
    create_time TIMESTAMP WITHOUT TIME ZONE,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP WITHOUT TIME ZONE,
    UNIQUE (fid, address)
);

CREATE INDEX idx_fc_verified_timestamp ON farcaster_verified_address (fid, create_time);
CREATE INDEX idx_fc_verified_address ON farcaster_verified_address (network, address);

CREATE TABLE farcaster_social (
    fid BIGINT NOT NULL,
    is_power BOOLEAN DEFAULT FALSE,
    follower INT DEFAULT 0,
    following INT DEFAULT 0,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (fid)
);

CREATE INDEX idx_fc_social_follower ON farcaster_social (follower);

BEGIN;
TRUNCATE TABLE public.farcaster_social;
COMMIT;