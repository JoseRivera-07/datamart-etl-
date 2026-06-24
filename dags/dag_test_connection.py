from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

with DAG(
    dag_id='test_connection',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    def test_external(**context):
        hook = PostgresHook(postgres_conn_id='postgres_external')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT current_database(), current_user;")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"Conectado a: {result[0]} como usuario: {result[1]}")

    from airflow.operators.python import PythonOperator
    PythonOperator(
        task_id='test_external_connection',
        python_callable=test_external
    )