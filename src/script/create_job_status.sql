CREATE TABLE job_status (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    job_type VARCHAR(255) NOT NULL,
    check_point BIGINT DEFAULT 0,
    job_status_type INT DEFAULT 0,
    job_status VARCHAR(255),
    update_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX job_name_index ON job_status (job_name);
CREATE INDEX job_type_index ON job_status (job_type);

BEGIN;
TRUNCATE TABLE public.job_status;
ALTER SEQUENCE public.job_status_id_seq RESTART WITH 1;
COMMIT;