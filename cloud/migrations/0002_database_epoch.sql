CREATE TABLE IF NOT EXISTS cloud_database_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  epoch TEXT NOT NULL CHECK (
    length(epoch) = 32 AND epoch NOT GLOB '*[^0-9a-f]*'
  )
);

INSERT OR IGNORE INTO cloud_database_meta (id, epoch)
VALUES (1, lower(hex(randomblob(16))));
