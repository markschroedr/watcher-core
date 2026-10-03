CREATE TABLE IF NOT EXISTS progress (
 watch TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, source_identity TEXT NOT NULL,
 cursor_json TEXT NOT NULL DEFAULT '{}', turns_json TEXT NOT NULL DEFAULT '[]',
 revision INTEGER NOT NULL DEFAULT 0, wake_seq INTEGER NOT NULL DEFAULT 0, spent_usd REAL NOT NULL DEFAULT 0,
 terminal_reason TEXT NOT NULL DEFAULT '', loaded_generation INTEGER,
 state TEXT NOT NULL DEFAULT 'running', last_error TEXT NOT NULL DEFAULT '', heartbeat REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS findings (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, watch TEXT NOT NULL,
 generation INTEGER, criterion TEXT NOT NULL, summary TEXT NOT NULL, evidence TEXT NOT NULL,
 action TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('ok','rejected')),
 labels_json TEXT NOT NULL, observed_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS findings_watch_seq ON findings(watch,seq);
CREATE TABLE IF NOT EXISTS deliveries (
 id TEXT PRIMARY KEY REFERENCES findings(id), kind TEXT NOT NULL, action_json TEXT NOT NULL,
 next_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
 repeat_seconds REAL NOT NULL DEFAULT 0, delivered_at REAL, acknowledged_at REAL, cancelled_at REAL
);
CREATE TABLE IF NOT EXISTS judgments (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, watch TEXT NOT NULL, phase TEXT NOT NULL,
 model TEXT NOT NULL, service_tier TEXT NOT NULL, request_id TEXT NOT NULL,
 usage_json TEXT NOT NULL, cost_usd REAL NOT NULL, error TEXT NOT NULL,
 trimmed_unjudged_chars INTEGER NOT NULL DEFAULT 0, observed_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS wakes (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, watch TEXT NOT NULL, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS daemon (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), pid INTEGER NOT NULL, heartbeat REAL NOT NULL,
 alive INTEGER NOT NULL
);
