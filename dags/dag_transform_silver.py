from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable, DagRun
from airflow.utils.state import State
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import pandas as pd
import logging

default_args = {
    'owner': 'datamart',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_transform_silver',
    description='Transformación Bronze → Silver con reglas de negocio DataMart',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def check_bronze_success(**context):
        runs = DagRun.find(dag_id='dag_ingest_bronze', state=State.SUCCESS)
        if not runs:
            raise ValueError("dag_ingest_bronze no tiene runs exitosos.")
        logging.info(f"Bronze exitoso: {runs[-1].execution_date}")

    def transform_and_load(**context):
        bronze_schema = Variable.get('bronze_schema')
        silver_schema = Variable.get('silver_schema')
        execution_date = context['ds']

        hook = PostgresHook(postgres_conn_id='postgres_dw')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Idempotencia
        cursor.execute(f"""
            DELETE FROM {silver_schema}.transactions
            WHERE DATE(processed_at) = %s
        """, (execution_date,))
        cursor.execute(f"""
            DELETE FROM {bronze_schema}.rejected_records
            WHERE DATE(rejected_at) = %s AND layer = 'silver'
        """, (execution_date,))
        conn.commit()

        seen_keys = set()
        total_valid = 0
        total_rejected = 0

        # CSV1 primero (tiene prioridad), luego CSV2
        for source_file in ['data.csv', 'online_retail.csv']:
            csv_cursor = conn.cursor()
            csv_cursor.execute(f"""
                SELECT invoice_no, stock_code, description, quantity,
                       invoice_date, unit_price, customer_id, country, source_file
                FROM {bronze_schema}.transactions
                WHERE source_file = %s
            """, (source_file,))

            columns = ['invoice_no', 'stock_code', 'description', 'quantity',
                       'invoice_date', 'unit_price', 'customer_id', 'country', 'source_file']

            while True:
                rows = csv_cursor.fetchmany(5000)
                if not rows:
                    break

                df = pd.DataFrame(rows, columns=columns)
                valid_records = []
                rejected_records = []

                for _, row in df.iterrows():
                    reason = None

                    try:
                        quantity = int(row['quantity'])
                    except Exception:
                        reason = f"quantity no numérico: {row['quantity']}"

                    try:
                        unit_price = float(row['unit_price'])
                    except Exception:
                        if not reason:
                            reason = f"unit_price no numérico: {row['unit_price']}"

                    try:
                        invoice_date = pd.to_datetime(row['invoice_date'], utc=True)
                    except Exception:
                        if not reason:
                            reason = f"invoice_date inválido: {row['invoice_date']}"

                    if reason:
                        rejected_records.append((
                            row['source_file'], str(dict(row)), reason, 'silver'
                        ))
                        continue

                    # Regla: UnitPrice <= 0 → rechazar
                    if unit_price <= 0:
                        rejected_records.append((
                            row['source_file'], str(dict(row)),
                            f"unit_price inválido: {unit_price}", 'silver'
                        ))
                        continue

                    # Normalización
                    stock_code = str(row['stock_code']).upper().strip()
                    description = str(row['description']).upper().strip()
                    if description in ('NAN', 'NONE', ''):
                        description = 'SIN DESCRIPCION'

                    customer_id = str(row['customer_id']).strip().upper()
                    if customer_id in ('NAN', 'NONE', ''):
                        customer_id = 'ANONYMOUS'

                    # Deduplicación entre fuentes
                    dedup_key = (
                        str(row['invoice_no']).strip(),
                        stock_code,
                        str(invoice_date.date())
                    )
                    if dedup_key in seen_keys:
                        rejected_records.append((
                            row['source_file'], str(dict(row)),
                            'Duplicado entre fuentes', 'silver'
                        ))
                        continue
                    seen_keys.add(dedup_key)

                    # Regla: Quantity <= 0 → devolución
                    transaction_type = 'return' if quantity <= 0 else 'sale'
                    gross_revenue = quantity * unit_price

                    valid_records.append((
                        str(row['invoice_no']).strip(),
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        unit_price,
                        customer_id,
                        str(row['country']).strip(),
                        transaction_type,
                        gross_revenue,
                        row['source_file']
                    ))

                if valid_records:
                    execute_values(cursor, f"""
                        INSERT INTO {silver_schema}.transactions
                        (invoice_no, stock_code, description, quantity,
                         invoice_date, unit_price, customer_id, country,
                         transaction_type, gross_revenue, source_file)
                        VALUES %s
                    """, valid_records, page_size=1000)

                if rejected_records:
                    execute_values(cursor, f"""
                        INSERT INTO {bronze_schema}.rejected_records
                        (source_file, raw_data, rejection_reason, layer)
                        VALUES %s
                    """, rejected_records, page_size=1000)

                conn.commit()
                total_valid += len(valid_records)
                total_rejected += len(rejected_records)

            csv_cursor.close()
            logging.info(f"{source_file} procesado.")

        cursor.close()
        conn.close()
        logging.info(f"Total válidos: {total_valid} | Total rechazados: {total_rejected}")

    t_check = PythonOperator(
        task_id='check_bronze_success',
        python_callable=check_bronze_success
    )

    t_transform = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load
    )

    t_check >> t_transform