-- ============================================================
-- Bronze Layer: réplica cruda de las fuentes
-- ============================================================
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.transactions (
    invoice_no      VARCHAR(50),
    stock_code      VARCHAR(50),
    description     TEXT,
    quantity        VARCHAR(50),
    invoice_date    VARCHAR(50),
    unit_price      VARCHAR(50),
    customer_id     VARCHAR(50),
    country         VARCHAR(100),
    source_file     VARCHAR(100),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.rejected_records (
    id               SERIAL PRIMARY KEY,
    source_file      VARCHAR(100),
    raw_data         TEXT,
    rejection_reason VARCHAR(255),
    layer            VARCHAR(20),
    rejected_at      TIMESTAMP DEFAULT NOW()
);