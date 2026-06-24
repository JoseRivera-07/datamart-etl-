-- ============================================================
-- DataMart S.A.S. - Consultas de validación
-- ============================================================

-- Pregunta 1: evolución mensual de ventas netas
SELECT year, month, gross_revenue, total_returns, net_revenue
FROM gold.monthly_sales
ORDER BY year, month;

-- Pregunta 2: categorías con más revenue bruto y mayor proporción de devoluciones
SELECT stock_code, description, total_sold, total_returned, net_revenue, return_rate
FROM gold.product_stats
ORDER BY total_sold DESC
LIMIT 10;

-- Pregunta 3: top 10 productos por revenue neto
SELECT stock_code, description, net_revenue
FROM gold.product_stats
ORDER BY net_revenue DESC
LIMIT 10;

-- Pregunta 3: top 10 productos por tasa de devolución
SELECT stock_code, description, return_rate
FROM gold.product_stats
ORDER BY return_rate DESC
LIMIT 10;

-- Pregunta 4: países con más transacciones y ticket promedio
SELECT country, total_transactions, avg_ticket
FROM gold.country_stats
ORDER BY total_transactions DESC;

-- Pregunta 5: clientes identificados vs anónimos
SELECT customer_type, total_transactions, total_revenue, avg_ticket
FROM gold.customer_stats;

-- Pregunta 6: productos con descripciones inconsistentes
SELECT stock_code, description_count, canonical_name
FROM gold.product_description_issues
ORDER BY description_count DESC
LIMIT 10;

-- Pregunta 6: total de códigos únicos de producto
SELECT COUNT(DISTINCT stock_code) AS total_productos
FROM silver.transactions;