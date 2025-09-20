-- Database Population Script for Penguinarium Backend
-- This SQL file populates the database with sample data sources, tables, fields, and alerts
-- 
-- Usage:
--   psql -d your_database_name -f populate_database.sql
--   or
--   mysql -u username -p database_name < populate_database.sql
--
-- Note: This script assumes the database tables already exist (run migrations first)

-- ============================================================================
-- SAMPLE USERS
-- ============================================================================

-- Insert sample users
INSERT INTO auth_user (username, email, password, first_name, last_name, is_staff, is_active, is_superuser, date_joined)
VALUES 
    ('user1', 'user1@example.com', 'pbkdf2_sha256$600000$dummy$dummy', 'John', 'Doe', false, true, false, NOW()),
    ('user2', 'user2@example.com', 'pbkdf2_sha256$600000$dummy$dummy', 'Jane', 'Smith', false, true, false, NOW()),
    ('user3', 'user3@example.com', 'pbkdf2_sha256$600000$dummy$dummy', 'Bob', 'Johnson', false, true, false, NOW())
ON CONFLICT (username) DO NOTHING;

-- ============================================================================
-- API KEYS
-- ============================================================================

-- Insert API keys for users
INSERT INTO api_apikey (key, user_id, created_at, revoked)
VALUES 
    ('pk_test_aaaaaaaaaaaaaaaaaaaaaaaaaa01', (SELECT id FROM auth_user WHERE username = 'user1'), NOW(), false),
    ('pk_test_bbbbbbbbbbbbbbbbbbbbbbbbbbbb02', (SELECT id FROM auth_user WHERE username = 'user2'), NOW(), false),
    ('pk_test_cccccccccccccccccccccccccccc03', (SELECT id FROM auth_user WHERE username = 'user3'), NOW(), false)
ON CONFLICT (key) DO NOTHING;

-- ============================================================================
-- DATA SOURCES
-- ============================================================================

-- Insert sample data sources
INSERT INTO pulling_datasource (name, type, connection_info, user_id, created_at, updated_at, global_id, is_deleted)
VALUES 
    (
        'Production PostgreSQL',
        'database',
        '{"host": "prod-db.example.com", "port": 5432, "database": "production", "username": "readonly_user", "ssl_mode": "require", "connection_pool_size": 10}',
        (SELECT id FROM auth_user WHERE username = 'user1'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'Analytics MySQL',
        'database',
        '{"host": "analytics-db.example.com", "port": 3306, "database": "analytics", "username": "analytics_user", "charset": "utf8mb4", "connection_pool_size": 5}',
        (SELECT id FROM auth_user WHERE username = 'user1'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'Customer API',
        'api',
        '{"base_url": "https://api.customers.example.com/v1", "auth_type": "bearer", "rate_limit": 1000, "timeout": 30, "retry_attempts": 3}',
        (SELECT id FROM auth_user WHERE username = 'user2'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'Sales Data Files',
        'file',
        '{"storage_type": "s3", "bucket": "sales-data-bucket", "region": "us-west-2", "file_format": "parquet", "compression": "snappy"}',
        (SELECT id FROM auth_user WHERE username = 'user2'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'Real-time Events Stream',
        'stream',
        '{"stream_type": "kafka", "bootstrap_servers": ["kafka1.example.com:9092", "kafka2.example.com:9092"], "topic": "user_events", "consumer_group": "penguinarium_analytics", "auto_offset_reset": "latest"}',
        (SELECT id FROM auth_user WHERE username = 'user3'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'Cloud Storage Bucket',
        'cloud',
        '{"provider": "aws_s3", "bucket": "data-lake-bucket", "region": "us-east-1", "access_pattern": "read_only", "encryption": "AES256"}',
        (SELECT id FROM auth_user WHERE username = 'user3'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    )
ON CONFLICT (global_id) DO NOTHING;

-- ============================================================================
-- TABLE METADATA
-- ============================================================================

-- Insert table metadata for Production PostgreSQL
INSERT INTO pulling_tablemetadata (name, description, metadata, data_source_id, created_at, updated_at, global_id, is_deleted)
VALUES 
    (
        'users',
        'User accounts and profile information',
        '{"schema": "public", "row_count": 15000, "size_mb": 25.5, "last_updated": "2024-01-15T10:30:00Z"}',
        (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'orders',
        'Customer orders and transaction data',
        '{"schema": "public", "row_count": 45000, "size_mb": 78.2, "last_updated": "2024-01-15T11:45:00Z"}',
        (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'products',
        'Product catalog and inventory',
        '{"schema": "public", "row_count": 2500, "size_mb": 12.8, "last_updated": "2024-01-15T09:15:00Z"}',
        (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    -- Analytics MySQL tables
    (
        'daily_metrics',
        'Daily aggregated business metrics',
        '{"schema": "analytics", "row_count": 365, "size_mb": 2.1, "last_updated": "2024-01-15T00:05:00Z"}',
        (SELECT data_source_id FROM pulling_datasource WHERE name = 'Analytics MySQL'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'user_behavior',
        'User interaction and behavior tracking',
        '{"schema": "analytics", "row_count": 125000, "size_mb": 156.7, "last_updated": "2024-01-15T12:00:00Z"}',
        (SELECT data_source_id FROM pulling_datasource WHERE name = 'Analytics MySQL'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    -- Sales Data Files
    (
        'sales_2024_q1',
        'Q1 2024 sales data in Parquet format',
        '{"file_path": "s3://sales-data-bucket/2024/q1/sales.parquet", "file_size_mb": 45.3, "record_count": 12000, "created_at": "2024-04-01T00:00:00Z"}',
        (SELECT data_source_id FROM pulling_datasource WHERE name = 'Sales Data Files'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'customer_segments',
        'Customer segmentation analysis results',
        '{"file_path": "s3://sales-data-bucket/analysis/customer_segments.parquet", "file_size_mb": 8.7, "record_count": 5000, "created_at": "2024-01-10T14:30:00Z"}',
        (SELECT data_source_id FROM pulling_datasource WHERE name = 'Sales Data Files'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    )
ON CONFLICT (global_id) DO NOTHING;

-- ============================================================================
-- FIELD METADATA
-- ============================================================================

-- Insert field metadata for users table
INSERT INTO pulling_fieldmetadata (name, dtype, metadata, table_id, created_at, updated_at, global_id, is_deleted)
VALUES 
    (
        'user_id',
        'integer',
        '{"primary_key": true, "auto_increment": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'email',
        'string',
        '{"unique": true, "max_length": 255, "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'first_name',
        'string',
        '{"max_length": 100, "nullable": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'last_name',
        'string',
        '{"max_length": 100, "nullable": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'created_at',
        'datetime',
        '{"nullable": false, "default": "CURRENT_TIMESTAMP"}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'is_active',
        'boolean',
        '{"default": true, "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'last_login',
        'datetime',
        '{"nullable": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    )
ON CONFLICT (global_id) DO NOTHING;

-- Insert field metadata for orders table
INSERT INTO pulling_fieldmetadata (name, dtype, metadata, table_id, created_at, updated_at, global_id, is_deleted)
VALUES 
    (
        'order_id',
        'integer',
        '{"primary_key": true, "auto_increment": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'user_id',
        'integer',
        '{"foreign_key": "users.user_id", "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'product_id',
        'integer',
        '{"foreign_key": "products.product_id", "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'quantity',
        'integer',
        '{"nullable": false, "min_value": 1}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'unit_price',
        'decimal',
        '{"precision": 10, "scale": 2, "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'total_amount',
        'decimal',
        '{"precision": 10, "scale": 2, "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'order_date',
        'datetime',
        '{"nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'status',
        'string',
        '{"max_length": 20, "choices": ["pending", "confirmed", "shipped", "delivered", "cancelled"]}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    )
ON CONFLICT (global_id) DO NOTHING;

-- Insert field metadata for products table
INSERT INTO pulling_fieldmetadata (name, dtype, metadata, table_id, created_at, updated_at, global_id, is_deleted)
VALUES 
    (
        'product_id',
        'integer',
        '{"primary_key": true, "auto_increment": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'name',
        'string',
        '{"max_length": 255, "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'description',
        'text',
        '{"nullable": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'price',
        'decimal',
        '{"precision": 10, "scale": 2, "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'category',
        'string',
        '{"max_length": 100, "nullable": true}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'in_stock',
        'boolean',
        '{"default": true, "nullable": false}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    ),
    (
        'stock_quantity',
        'integer',
        '{"nullable": false, "min_value": 0}',
        (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products'),
        NOW(),
        NOW(),
        gen_random_uuid(),
        false
    )
ON CONFLICT (global_id) DO NOTHING;

-- ============================================================================
-- SAMPLE ALERT DATA (JSON format for future Alert model)
-- ============================================================================

/*
Sample Alert Data (to be used when Alert model is implemented):

[
  {
    "id": "alert_001",
    "datasource_id": "ds_[data_source_global_id_prefix]",
    "name": "Database Connection High Latency",
    "severity": "warning",
    "status": "active",
    "details": {
      "metric": "connection_latency",
      "threshold": 1000,
      "current_value": 1250,
      "unit": "ms"
    },
    "triggered_at": "2024-01-15T10:30:00Z"
  },
  {
    "id": "alert_002",
    "datasource_id": "ds_[data_source_global_id_prefix]",
    "name": "Database Disk Space Low",
    "severity": "critical",
    "status": "active",
    "details": {
      "metric": "disk_usage_percent",
      "threshold": 90,
      "current_value": 94.5,
      "unit": "%"
    },
    "triggered_at": "2024-01-15T09:15:00Z"
  },
  {
    "id": "alert_003",
    "datasource_id": "ds_[data_source_global_id_prefix]",
    "name": "API Rate Limit Exceeded",
    "severity": "warning",
    "status": "resolved",
    "details": {
      "metric": "requests_per_minute",
      "threshold": 100,
      "current_value": 95,
      "unit": "req/min"
    },
    "triggered_at": "2024-01-15T08:45:00Z"
  },
  {
    "id": "alert_004",
    "datasource_id": "ds_[data_source_global_id_prefix]",
    "name": "Kafka Consumer Lag",
    "severity": "info",
    "status": "active",
    "details": {
      "metric": "consumer_lag",
      "threshold": 1000,
      "current_value": 750,
      "unit": "messages"
    },
    "triggered_at": "2024-01-15T11:20:00Z"
  }
]

*/

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Display summary of created data
SELECT 'Users' as table_name, COUNT(*) as count FROM auth_user WHERE username LIKE 'user%'
UNION ALL
SELECT 'API Keys', COUNT(*) FROM api_apikey WHERE key LIKE 'pk_test_%'
UNION ALL
SELECT 'Data Sources', COUNT(*) FROM pulling_datasource WHERE is_deleted = false
UNION ALL
SELECT 'Tables', COUNT(*) FROM pulling_tablemetadata WHERE is_deleted = false
UNION ALL
SELECT 'Fields', COUNT(*) FROM pulling_fieldmetadata WHERE is_deleted = false;

-- Display API keys for testing
SELECT 
    u.username,
    u.email,
    ak.key as api_key
FROM auth_user u
JOIN api_apikey ak ON u.id = ak.user_id
WHERE u.username LIKE 'user%'
ORDER BY u.username;

-- Display data source IDs for testing
SELECT 
    ds.name,
    'ds_' || SUBSTRING(ds.global_id::text, 1, 10) as datasource_id
FROM pulling_datasource ds
WHERE ds.is_deleted = false
ORDER BY ds.name;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================

-- Script completed successfully!
-- You can now test the API endpoints with the provided API keys and data source IDs.
