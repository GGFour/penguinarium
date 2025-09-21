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
-- ALERTS (seed data)
-- ============================================================================

-- Notes:
-- - Matches pulling.Alert schema (data_source, optional table/field, enums for severity/status)
-- - Uses Postgres JSON, timestamps, and gen_random_uuid() for global_id
-- - Duplicate-safe via ON CONFLICT (global_id) DO NOTHING

INSERT INTO pulling_alert (
        name, severity, status, details, triggered_at,
        data_source_id, table_id, field_id,
        created_at, updated_at, global_id, is_deleted
)
VALUES
        -- Production PostgreSQL -> users.last_login has high NULL rate (too many missing values)
        (
                'High NULL rate in users.last_login',
                'warning',
                'active',
                '{"metric": "null_rate", "row_count": 15000, "null_count": 8200, "null_rate": 0.5467, "threshold": 0.30, "field": "last_login", "table": "users"}',
                NOW() - INTERVAL '2 days',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
                (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL')),
                (SELECT field_metadata_id FROM pulling_fieldmetadata WHERE name = 'last_login' AND table_id = (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'))),
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Production PostgreSQL -> users.email has pattern mismatches (value vs column semantics)
        (
                'Email format mismatches in users.email',
                'warning',
                'active',
                '{"metric": "pattern_mismatch", "expected_pattern": "^.+@.+\\..+$", "mismatch_count": 275, "sample_values": ["john_at_example.com", "no_at_symbol"], "field": "email", "table": "users"}',
                NOW() - INTERVAL '1 day',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
                (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL')),
                (SELECT field_metadata_id FROM pulling_fieldmetadata WHERE name = 'email' AND table_id = (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'users' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'))),
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Production PostgreSQL -> orders table has fewer rows than expected (too many missing rows)
        (
                'Missing rows in orders for 2024-01-14',
                'critical',
                'active',
                '{"metric": "row_count_gap", "date": "2024-01-14", "expected_rows": 2000, "actual_rows": 1240, "gap": 760, "gap_pct": 0.38, "table": "orders"}',
                NOW() - INTERVAL '3 days',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
                (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL')),
                NULL,
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Production PostgreSQL -> orders.total_amount outliers
        (
                'Outliers detected in orders.total_amount',
                'warning',
                'active',
                '{"metric": "outliers", "method": "zscore", "threshold": 3.0, "outlier_count": 95, "pctl_99": 14250.00, "max": 99999.99, "field": "total_amount", "table": "orders"}',
                NOW() - INTERVAL '12 hours',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
                (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL')),
                (SELECT field_metadata_id FROM pulling_fieldmetadata WHERE name = 'total_amount' AND table_id = (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'orders' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'))),
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Production PostgreSQL -> products.price invalid negatives (mismatch with semantics)
        (
                'Negative values found in products.price',
                'critical',
                'active',
                '{"metric": "invalid_values", "condition": "price < 0", "violations": 3, "min": -12.50, "field": "price", "table": "products"}',
                NOW() - INTERVAL '6 hours',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'),
                (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL')),
                (SELECT field_metadata_id FROM pulling_fieldmetadata WHERE name = 'price' AND table_id = (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'products' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Production PostgreSQL'))),
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Analytics MySQL -> daily_metrics has gaps in daily dates (missing data)
        (
                'Date gaps detected in analytics.daily_metrics',
                'warning',
                'active',
                '{"metric": "date_gaps", "expected_frequency": "daily", "missing_days": ["2024-01-05", "2024-01-12", "2024-01-19"], "gap_count": 3, "table": "daily_metrics"}',
                NOW() - INTERVAL '4 days',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Analytics MySQL'),
                (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'daily_metrics' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Analytics MySQL')),
                NULL,
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Sales Data Files -> sales_2024_q1 high nulls in a column (table-level alert without FK to field)
        (
                'High NULL rate in sales_2024_q1.unit_price',
                'warning',
                'active',
                '{"metric": "null_rate", "field": "unit_price", "row_count": 12000, "null_count": 2760, "null_rate": 0.23, "threshold": 0.10, "table": "sales_2024_q1"}',
                NOW() - INTERVAL '5 days',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Sales Data Files'),
                (SELECT table_metadata_id FROM pulling_tablemetadata WHERE name = 'sales_2024_q1' AND data_source_id = (SELECT data_source_id FROM pulling_datasource WHERE name = 'Sales Data Files')),
                NULL,
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Customer API -> schema drift (data-source-level)
        (
                'Customer API schema drift detected',
                'info',
                'active',
                '{"metric": "schema_drift", "endpoint": "/v1/customers", "added_fields": ["middle_name"], "removed_fields": ["age"], "breaking": true}',
                NOW() - INTERVAL '7 days',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Customer API'),
                NULL,
                NULL,
                NOW(), NOW(), gen_random_uuid(), false
        ),
        -- Real-time Events Stream -> consumer lag
        (
                'Kafka consumer lag high',
                'warning',
                'resolved',
                '{"metric": "consumer_lag", "topic": "user_events", "current": 750, "threshold": 500, "unit": "messages"}',
                NOW() - INTERVAL '8 days',
                (SELECT data_source_id FROM pulling_datasource WHERE name = 'Real-time Events Stream'),
                NULL,
                NULL,
                NOW(), NOW(), gen_random_uuid(), false
        )
ON CONFLICT (global_id) DO NOTHING;

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
SELECT 'Fields', COUNT(*) FROM pulling_fieldmetadata WHERE is_deleted = false
UNION ALL
SELECT 'Alerts', COUNT(*) FROM pulling_alert WHERE is_deleted = false;

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
