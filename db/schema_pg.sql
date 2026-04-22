-- Memphis Blight Compass - Postgres schema (Neon-compatible)

CREATE TABLE IF NOT EXISTS requests_311 (
    incident_number TEXT PRIMARY KEY,
    objectid BIGINT,
    parcel_id TEXT,
    parcel_norm TEXT,
    category TEXT,
    group_name TEXT,
    department TEXT,
    division TEXT,
    request_type TEXT,
    request_status TEXT,
    request_priority TEXT,
    reported_date TIMESTAMPTZ,
    closed_date TIMESTAMPTZ,
    resolved_date TIMESTAMPTZ,
    days_old INTEGER,
    address TEXT,
    zipcode TEXT,
    neighborhood TEXT,
    council_district TEXT,
    creation_channel TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_req311_parcel ON requests_311(parcel_norm);
CREATE INDEX IF NOT EXISTS idx_req311_zip    ON requests_311(zipcode);
CREATE INDEX IF NOT EXISTS idx_req311_date   ON requests_311(reported_date);
CREATE INDEX IF NOT EXISTS idx_req311_status ON requests_311(request_status);

CREATE TABLE IF NOT EXISTS code_violations (
    id TEXT PRIMARY KEY,
    source_layer TEXT NOT NULL,
    case_number TEXT,
    parcel_id TEXT,
    parcel_norm TEXT,
    violation_type TEXT,
    status TEXT,
    open_date TIMESTAMPTZ,
    close_date TIMESTAMPTZ,
    address TEXT,
    zipcode TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_cv_parcel ON code_violations(parcel_norm);
CREATE INDEX IF NOT EXISTS idx_cv_zip    ON code_violations(zipcode);
CREATE INDEX IF NOT EXISTS idx_cv_date   ON code_violations(open_date);

CREATE TABLE IF NOT EXISTS landbank_inventory (
    parcel_id TEXT PRIMARY KEY,
    parcel_norm TEXT,
    address TEXT,
    zipcode TEXT,
    current_status TEXT,
    available TEXT,
    asking_price DOUBLE PRECISION,
    acres DOUBLE PRECISION,
    parcel_length DOUBLE PRECISION,
    parcel_width DOUBLE PRECISION,
    improvement_type TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    last_seen_at TIMESTAMPTZ,
    raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_lb_norm   ON landbank_inventory(parcel_norm);
CREATE INDEX IF NOT EXISTS idx_lb_zip    ON landbank_inventory(zipcode);
CREATE INDEX IF NOT EXISTS idx_lb_status ON landbank_inventory(current_status, available);

CREATE TABLE IF NOT EXISTS flood_zones (
    parcel_id TEXT PRIMARY KEY,
    parcel_norm TEXT,
    flood_zone TEXT,
    sfha_tf TEXT,
    static_bfe DOUBLE PRECISION,
    checked_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_flood_norm ON flood_zones(parcel_norm);

CREATE TABLE IF NOT EXISTS scores (
    parcel_id TEXT PRIMARY KEY,
    parcel_norm TEXT,
    score DOUBLE PRECISION NOT NULL,
    chronic_complaints INTEGER DEFAULT 0,
    code_violations   INTEGER DEFAULT 0,
    flood_safe  INTEGER DEFAULT 0,
    affordable  INTEGER DEFAULT 0,
    buildable   INTEGER DEFAULT 0,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    computed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_scores_score ON scores(score DESC);
CREATE INDEX IF NOT EXISTS idx_scores_norm  ON scores(parcel_norm);

CREATE TABLE IF NOT EXISTS subscribers (
    email TEXT PRIMARY KEY,
    subscribed_at TIMESTAMPTZ,
    confirmed INTEGER DEFAULT 0,
    unsubscribe_token TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    records_inserted INTEGER DEFAULT 0,
    records_updated  INTEGER DEFAULT 0,
    status TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_inglog_source ON ingestion_log(source, started_at DESC);
