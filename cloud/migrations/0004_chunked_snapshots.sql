ALTER TABLE snapshot_meta
  ADD COLUMN transport TEXT NOT NULL DEFAULT 'legacy'
    CHECK (transport IN ('legacy', 'chunked-v1'));

ALTER TABLE snapshot_meta ADD COLUMN library_manifest_json TEXT;
ALTER TABLE snapshot_meta ADD COLUMN state_manifest_json TEXT;

CREATE TABLE IF NOT EXISTS snapshot_content_chunks (
  database_epoch TEXT NOT NULL CHECK (
    length(database_epoch) = 32 AND database_epoch NOT GLOB '*[^0-9a-f]*'
  ),
  sha256 TEXT NOT NULL CHECK (
    length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'
  ),
  byte_length INTEGER NOT NULL CHECK (byte_length BETWEEN 1 AND 32768),
  data TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (database_epoch, sha256)
);

CREATE INDEX IF NOT EXISTS snapshot_content_chunks_gc
  ON snapshot_content_chunks (created_at, database_epoch, sha256);
