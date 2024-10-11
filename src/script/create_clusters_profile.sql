CREATE TABLE clusters_profile (
    id SERIAL PRIMARY KEY,
    cluster_id BIGINT NOT NULL,
    bytes32_address VARCHAR(255) NOT NULL,
    network VARCHAR(66),
    address VARCHAR(255) NOT NULL,
    address_type VARCHAR(255) NOT NULL,
    is_verified BOOLEAN DEFAULT FALSE,
    cluster_name VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    avatar text,  -- imageUrl
    display_name TEXT, -- 2
    description TEXT, -- 3
    texts JSONB DEFAULT '{}'::jsonb,
    registration_time TIMESTAMP WITHOUT TIME ZONE,
    create_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT unique_clusters_profile UNIQUE (cluster_id, address, address_type, cluster_name)
);

CREATE INDEX clusters_profile_idx_address ON clusters_profile (address);
CREATE INDEX clusters_profile_idx_cluster_name ON clusters_profile (cluster_name);
CREATE INDEX clusters_profile_idx_subname ON clusters_profile (name);
CREATE INDEX clusters_profile_idx_cluster_id ON clusters_profile (cluster_id);


BEGIN;
TRUNCATE TABLE public.clusters_profile;
ALTER SEQUENCE public.clusters_profile_id_seq RESTART WITH 1;
COMMIT;