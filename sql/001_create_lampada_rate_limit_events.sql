CREATE TABLE IF NOT EXISTS lampada_rate_limit_events (
    code BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
        COMMENT 'Rate limit event code',
    client_hash BINARY(32) NOT NULL
        COMMENT 'HMAC-SHA-256 pseudonym of the client address',
    dt_create DATETIME(6) NOT NULL
        COMMENT 'UTC reservation timestamp',
    INDEX idx_lampada_rate_limit_dt_create (dt_create)
        COMMENT 'Cleanup and global rolling-window lookup',
    INDEX idx_lampada_rate_limit_client_time (client_hash, dt_create)
        COMMENT 'Per-client rolling-window lookup'
) ENGINE=InnoDB
  COMMENT='Distributed Lampada AI rate limit reservations';
