CREATE TABLE IF NOT EXISTS inbox (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  dedup_key TEXT NOT NULL UNIQUE,
  payload_hash TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('telegram', 'library_scan', 'file_preview', 'file_confirm', 'legacy_import')),
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  acked_at INTEGER
);

CREATE INDEX IF NOT EXISTS inbox_pending_seq ON inbox (acked_at, seq);

CREATE TABLE IF NOT EXISTS snapshot_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  state_version INTEGER NOT NULL,
  reported_at INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  library_revision TEXT NOT NULL,
  library_hash TEXT NOT NULL,
  library_chunk_count INTEGER NOT NULL,
  state_hash TEXT NOT NULL,
  state_chunk_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_chunks (
  kind TEXT NOT NULL CHECK (kind IN ('library', 'state')),
  part INTEGER NOT NULL,
  data TEXT NOT NULL,
  PRIMARY KEY (kind, part)
);
