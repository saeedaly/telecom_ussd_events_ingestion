CREATE TABLE IF NOT EXISTS ussd_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    msisdn VARCHAR(20) NOT NULL,
    service_id_name VARCHAR(20) NOT NULL,
    service VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    source_file VARCHAR(255) NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ussd_event_source_file
    ON ussd_events (source_file);

-- CREATE TABLE IF NOT EXISTS file_ingestion_log (
--     id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
--     file_name VARCHAR(255) UNIQUE NOT NULL,
--     file_size_bytes BIGINT,
--     expected_rows BIGINT,
--     loaded_rows BIGINT,
--     status VARCHAR(30) NOT NULL DEFAULT 'DETECTED',
--     validation_status VARCHAR(30),
--     detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
--     ingestion_started_at TIMESTAMPTZ,
--     ingestion_completed_at TIMESTAMPTZ,
--     error_message TEXT
-- );
