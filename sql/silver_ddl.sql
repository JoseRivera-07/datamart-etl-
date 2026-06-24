-- ============================================================
-- Silver Layer: datos limpios y estandarizados
-- ============================================================
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.transactions (
    id               SERIAL PRIMARY KEY,
    invoice_no       VARCHAR(50),
    stock_code       VARCHAR(50),
    description      VARCHAR(255),
    quantity         INTEGER,
    invoice_date     TIMESTAMP WITH TIME ZONE,
    unit_price       NUMERIC(10,2),
    customer_id      VARCHAR(50),
    country          VARCHAR(100),
    transaction_type VARCHAR(10),
    gross_revenue    NUMERIC(12,2),
    source_file      VARCHAR(100),
    processed_at     TIMESTAMP DEFAULT NOW()
);