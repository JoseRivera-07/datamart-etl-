# DataMart ETL Pipeline

Pipeline ETL construido con Apache Airflow y PostgreSQL implementando arquitectura Medallion (Bronze → Silver → Gold) para procesar transacciones de ventas de DataMart S.A.S.

## Arquitectura

```
data.csv  ──┐
             ├──→ Bronze (réplica cruda)
online_retail.csv ──┘         ↓
                        Silver (limpio y validado)
                              ↓
                        Gold (agregado y analítico)
```

### Flujo de DAGs

```
dag_ingest_bronze
      ↓
dag_transform_silver
      ↓
dag_aggregate_gold
```

## Modelo de datos

### Bronze
Réplica exacta de ambos CSVs con columnas de auditoría. Todo en VARCHAR para preservar el dato crudo.

### Silver
Datos limpios con tipos correctos, `transaction_type` (sale/return) y `gross_revenue` calculado.

### Gold
5 tablas analíticas que responden las preguntas de negocio:
- `monthly_sales` — evolución mensual de ventas netas
- `product_stats` — revenue y tasa de devolución por producto
- `country_stats` — transacciones y ticket promedio por país
- `customer_stats` — clientes identificados vs anónimos
- `product_description_issues` — productos con descripciones inconsistentes

## Requisitos previos

- Docker y docker-compose instalados
- Python 3.8+
- Los dos CSVs descargados en `data/`:
  - `data.csv`: https://www.kaggle.com/datasets/carrie1/ecommerce-data
  - `online_retail.csv`: https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset

## Reproducir el entorno

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd datamart-etl
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
```

Edita `.env` y completa los valores. Para generar la Fernet Key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Dar permisos a las carpetas

```bash
sudo chmod -R 777 logs/ plugins/ dags/
```

### 4. Levantar el entorno

```bash
docker-compose up
```

Al levantar, el entorno configura automáticamente:
- Usuario admin de Airflow
- Connection `postgres_dw` al Data Warehouse
- Variables: `csv_path_1`, `csv_path_2`, `bronze_schema`, `silver_schema`, `gold_schema`

### 5. Acceder a Airflow

- URL: http://localhost:8080
- Usuario: `admin`
- Contraseña: `admin`

### 6. Ejecutar el pipeline

Triggerear los DAGs en orden:

1. `dag_ingest_bronze` → esperar success
2. `dag_transform_silver` → esperar success
3. `dag_aggregate_gold` → esperar success

### 7. Validar resultados

```bash
docker exec -it datamart-etl-_postgres-dw_1 psql -U dw_user -d datamart_dw
```

```sql
SELECT source_file, COUNT(*) FROM bronze.transactions GROUP BY source_file;
SELECT transaction_type, COUNT(*) FROM silver.transactions GROUP BY transaction_type;
SELECT * FROM gold.customer_stats;
```

## Decisiones técnicas

### Arquitectura Medallion
Bronze preserva el dato crudo sin transformación. Silver aplica reglas de negocio y limpieza. Gold agrega para consumo analítico. Si cambia una regla de negocio, se reprocesa Silver y Gold sin volver a ingestar Bronze.

### Bronze: todo en VARCHAR
Garantiza que ningún registro se pierda por error de tipo en la ingesta. La conversión ocurre en Silver.

### Casos ambiguos resueltos

**Transacciones sin CustomerID:** se incluyen con valor `ANONYMOUS`. Permiten responder la pregunta 5 del enunciado — los clientes anónimos tienen un ticket promedio 40% menor que los identificados.

**Descripción canónica:** se toma la versión en MAYÚSCULAS. Es la más frecuente en ambos datasets y elimina ambigüedad de capitalización.

**Solapamiento de fechas:** ambos datasets se solapan en diciembre 2010. CSV1 tiene prioridad — los duplicados detectados por clave `(invoice_no, stock_code, date)` provenientes de CSV2 se rechazan y se loguean. Resultado: 91,418 duplicados detectados.

**Quantity <= 0:** se trata como devolución con `transaction_type = 'return'` y `gross_revenue` negativo. Permite calcular revenue neto con `SUM(gross_revenue)`.

**UnitPrice <= 0:** se rechaza. No existe producto sin precio en una venta válida.

### Idempotencia
Cada tarea borra los registros del día de ejecución antes de insertar. Ejecutar cualquier DAG dos veces el mismo día produce el mismo resultado.

### Inicialización automática
Connections y Variables se configuran dentro del `airflow-init` al hacer `docker-compose up`. No se requiere intervención manual en la UI.

## Consultas SQL de validación

Ver `sql/validacion.sql` — responde las 7 preguntas de negocio del enunciado.