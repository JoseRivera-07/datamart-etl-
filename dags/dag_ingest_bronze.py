from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
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
    dag_id='dag_ingest_bronze',
    description='Ingesta de CSV1 y CSV2 a capa Bronze',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def ingest_csv(csv_path, source_file, **context):
        bronze_schema = Variable.get('bronze_schema')
        execution_date = context['ds']

        df = pd.read_csv(csv_path, encoding='latin1')
        logging.info(f"Filas leídas de {source_file}: {len(df)}")

        # Normalizar columnas al esquema unificado
        df = df.rename(columns={
            'InvoiceNo':   'invoice_no',
            'StockCode':   'stock_code',
            'Description': 'description',
            'Quantity':    'quantity',
            'InvoiceDate': 'invoice_date',
            'UnitPrice':   'unit_price',
            'CustomerID':  'customer_id',
            'Country':     'country',
            'Invoice':     'invoice_no',
            'Price':       'unit_price',
            'Customer ID': 'customer_id',
        })

        cols = ['invoice_no', 'stock_code', 'description',
                'quantity', 'invoice_date', 'unit_price',
                'customer_id', 'country']
        df = df[cols].astype(str)
        df['source_file'] = source_file

        hook = PostgresHook(postgres_conn_id='postgres_dw')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Idempotencia
        cursor.execute(f"""
            DELETE FROM {bronze_schema}.transactions
            WHERE source_file = %s AND DATE(ingested_at) = %s
        """, (source_file, execution_date))

        records = [
            (
                row['invoice_no'], row['stock_code'], row['description'],
                row['quantity'], row['invoice_date'], row['unit_price'],
                row['customer_id'], row['country'], row['source_file']
            )
            for _, row in df.iterrows()
        ]

        execute_values(cursor, f"""
            INSERT INTO {bronze_schema}.transactions
            (invoice_no, stock_code, description, quantity,
             invoice_date, unit_price, customer_id, country, source_file)
            VALUES %s
        """, records, page_size=1000)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Filas insertadas en bronze.transactions: {len(df)}")

    def ingest_csv1(**context):
        ingest_csv(Variable.get('csv_path_1'), 'data.csv', **context)

    def ingest_csv2(**context):
        ingest_csv(Variable.get('csv_path_2'), 'online_retail.csv', **context)

    t_csv1 = PythonOperator(task_id='ingest_csv1', python_callable=ingest_csv1)
    t_csv2 = PythonOperator(task_id='ingest_csv2', python_callable=ingest_csv2)

    t_csv1 >> t_csv2