CREATE TABLE inbox_with_preview_cancel_kind (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  dedup_key TEXT NOT NULL UNIQUE,
  payload_hash TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN (
    'telegram',
    'library_scan',
    'library_register',
    'library_set_default',
    'library_set_category',
    'library_rename',
    'library_remove',
    'metadata_recover',
    'file_preview',
    'file_confirm',
    'preview_cancel',
    'legacy_import',
    'download_submit',
    'library_migration_preview',
    'library_migration_confirm',
    'library_migration_control'
  )),
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  acked_at INTEGER
);

CREATE TABLE inbox_sequence_backup (seq INTEGER NOT NULL);
INSERT INTO inbox_sequence_backup (seq)
SELECT seq FROM sqlite_sequence WHERE name = 'inbox';

INSERT INTO inbox_with_preview_cancel_kind (seq, dedup_key, payload_hash, kind, payload_json, created_at, acked_at)
SELECT seq, dedup_key, payload_hash, kind, payload_json, created_at, acked_at
FROM inbox;

DROP TABLE inbox;
ALTER TABLE inbox_with_preview_cancel_kind RENAME TO inbox;

UPDATE sqlite_sequence
SET seq = MAX(seq, COALESCE((SELECT MAX(seq) FROM inbox_sequence_backup), 0))
WHERE name = 'inbox';

INSERT INTO sqlite_sequence (name, seq)
SELECT 'inbox', MAX(seq)
FROM inbox_sequence_backup
HAVING MAX(seq) IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'inbox');

DROP TABLE inbox_sequence_backup;

CREATE INDEX inbox_pending_seq ON inbox (acked_at, seq);
