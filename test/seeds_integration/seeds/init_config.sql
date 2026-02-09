-- SQL seed for initializing configuration table
CREATE TABLE IF NOT EXISTS app_config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO app_config (config_key, config_value) VALUES
    ('version', '1.0.0'),
    ('environment', 'test'),
    ('feature_flags', 'enabled');
