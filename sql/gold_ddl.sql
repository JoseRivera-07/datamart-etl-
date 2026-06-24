-- ============================================================
-- Gold Layer: tablas analíticas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.monthly_sales (
    year            INT,
    month           INT,
    gross_revenue   NUMERIC(12,2),
    total_returns   NUMERIC(12,2),
    net_revenue     NUMERIC(12,2),
    aggregated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.product_stats (
    stock_code      VARCHAR(50),
    description     VARCHAR(255),
    total_sold      NUMERIC(12,2),
    total_returned  NUMERIC(12,2),
    net_revenue     NUMERIC(12,2),
    return_rate     NUMERIC(5,2),
    aggregated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.country_stats (
    country             VARCHAR(100),
    total_transactions  INT,
    avg_ticket          NUMERIC(12,2),
    aggregated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.customer_stats (
    customer_type       VARCHAR(20),
    total_transactions  INT,
    total_revenue       NUMERIC(12,2),
    avg_ticket          NUMERIC(12,2),
    aggregated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.product_description_issues (
    stock_code          VARCHAR(50),
    description_count   INT,
    canonical_name      VARCHAR(255),
    aggregated_at       TIMESTAMP DEFAULT NOW()
);