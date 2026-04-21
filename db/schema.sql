-- Memphis Blight Compass schema

CREATE TABLE IF NOT EXISTS requests_311 (
    incident_number TEXT PRIMARY KEY,
    objectid INTEGER,
    parcel_id TEXT,
    category TEXT,
    group_name TEXT,
    department TEXT,
    division TEXT,
    request_type TEXT,
    request_status TEXT,
    request_priority TEXT,
    reported_date TEXT,
    closed_date TEXT,
    resolved_date TEXT,
    days_old INTEGER,
    address TEXT,
    zipcode TEXT,
    neighborhood TEXT,
    council_district TEXT,
    creation_channel TEXT,
    lat REAL,
    lng REAL
);
CREATE INDEX IF NOT EXISTS idx_req311_parcel ON requests_311(parcel_id);
CREATE INDEX IF NOT EXISTS idx_req311_zip ON requests_311(zipcode);
CREATE INDEX IF NOT EXISTS idx_req311_date ON requests_311(reported_date);
CREATE INDEX IF NOT EXISTS idx_req311_status ON requests_311(request_status);

CREATE TABLE IF NOT EXISTS code_violations (
    id TEXT PRIMARY KEY,
    source_layer TEXT NOT NULL,
    case_number TEXT,
    violation_type TEXT,
    status TEXT,
    open_date TEXT,
    close_date TEXT,
    address TEXT,
    zipcode TEXT,
    parcel_id TEXT,
    lat REAL,
    lng REAL,
    raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_cv_parcel ON code_violations(parcel_id);
CREATE INDEX IF NOT EXISTS idx_cv_zip ON code_violations(zipcode);
CREATE INDEX IF NOT EXISTS idx_cv_date ON code_violations(open_date);

CREATE TABLE IF NOT EXISTS landbank_inventory (
    parcel_id TEXT PRIMARY KEY,
    address TEXT,
    zipcode TEXT,
    current_status TEXT,
    available TEXT,
    asking_price REAL,
    acres REAL,
    parcel_length REAL,
    parcel_width REAL,
    improvement_type TEXT,
    lat REAL,
    lng REAL,
    last_seen_at TEXT,
    raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_lb_zip ON landbank_inventory(zipcode);
CREATE INDEX IF NOT EXISTS idx_lb_status ON landbank_inventory(current_status, available);

CREATE TABLE IF NOT EXISTS flood_zones (
    parcel_id TEXT PRIMARY KEY,
    flood_zone TEXT,
    sfha_tf TEXT,
    static_bfe REAL,
    checked_at TEXT
);

CREATE TABLE IF NOT EXISTS geocode_cache (
    address_key TEXT PRIMARY KEY,
    lat REAL,
    lng REAL,
    formatted_address TEXT,
    place_id TEXT,
    cached_at TEXT
);

CREATE TABLE IF NOT EXISTS scores (
    parcel_id TEXT PRIMARY KEY,
    score REAL NOT NULL,
    chronic_complaints INTEGER DEFAULT 0,
    code_violations INTEGER DEFAULT 0,
    flood_safe INTEGER DEFAULT 0,
    affordable INTEGER DEFAULT 0,
    buildable INTEGER DEFAULT 0,
    computed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_scores_score ON scores(score DESC);

CREATE TABLE IF NOT EXISTS subscribers (
    email TEXT PRIMARY KEY,
    subscribed_at TEXT,
    confirmed INTEGER DEFAULT 0,
    unsubscribe_token TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    status TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_inglog_source ON ingestion_log(source, started_at DESC);
