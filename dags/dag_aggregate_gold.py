from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable, DagRun
from airflow.utils.state import State
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'datamart',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_aggregate_gold',
    description='Agregación Silver → Gold',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def check_silver_success(**context):
        runs = DagRun.find(dag_id='dag_transform_silver', state=State.SUCCESS)
        if not runs:
            raise ValueError("dag_transform_silver no tiene runs exitosos.")
        logging.info(f"Silver exitoso: {runs[-1].execution_date}")

    def gold_monthly_sales(**context):
        hook = PostgresHook(postgres_conn_id='postgres_dw')
        silver_schema = Variable.get('silver_schema')
        gold_schema = Variable.get('gold_schema')
        execution_date = context['ds']
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {gold_schema}.monthly_sales WHERE DATE(aggregated_at) = %s", (execution_date,))

        cursor.execute(f"""
            INSERT INTO {gold_schema}.monthly_sales
            (year, month, gross_revenue, total_returns, net_revenue)
            SELECT
                EXTRACT(YEAR FROM invoice_date)::INT AS year,
                EXTRACT(MONTH FROM invoice_date)::INT AS month,
                SUM(CASE WHEN transaction_type = 'sale' THEN gross_revenue ELSE 0 END) AS gross_revenue,
                SUM(CASE WHEN transaction_type = 'return' THEN ABS(gross_revenue) ELSE 0 END) AS total_returns,
                SUM(gross_revenue) AS net_revenue
            FROM {silver_schema}.transactions
            GROUP BY year, month
            ORDER BY year, month
        """)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Gold monthly_sales generado.")

    def gold_product_stats(**context):
        hook = PostgresHook(postgres_conn_id='postgres_dw')
        silver_schema = Variable.get('silver_schema')
        gold_schema = Variable.get('gold_schema')
        execution_date = context['ds']
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {gold_schema}.product_stats WHERE DATE(aggregated_at) = %s", (execution_date,))

        cursor.execute(f"""
            INSERT INTO {gold_schema}.product_stats
            (stock_code, description, total_sold, total_returned, net_revenue, return_rate)
            SELECT
                stock_code,
                MAX(description) AS description,
                SUM(CASE WHEN transaction_type = 'sale' THEN gross_revenue ELSE 0 END) AS total_sold,
                SUM(CASE WHEN transaction_type = 'return' THEN ABS(gross_revenue) ELSE 0 END) AS total_returned,
                SUM(gross_revenue) AS net_revenue,
                ROUND(100.0 * COUNT(CASE WHEN transaction_type = 'return' THEN 1 END) / COUNT(*), 2) AS return_rate
            FROM {silver_schema}.transactions
            GROUP BY stock_code
            ORDER BY net_revenue DESC
        """)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Gold product_stats generado.")

    def gold_country_stats(**context):
        hook = PostgresHook(postgres_conn_id='postgres_dw')
        silver_schema = Variable.get('silver_schema')
        gold_schema = Variable.get('gold_schema')
        execution_date = context['ds']
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {gold_schema}.country_stats WHERE DATE(aggregated_at) = %s", (execution_date,))

        cursor.execute(f"""
            INSERT INTO {gold_schema}.country_stats
            (country, total_transactions, avg_ticket)
            SELECT
                country,
                COUNT(*) AS total_transactions,
                ROUND(AVG(gross_revenue), 2) AS avg_ticket
            FROM {silver_schema}.transactions
            WHERE transaction_type = 'sale'
            GROUP BY country
            ORDER BY total_transactions DESC
        """)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Gold country_stats generado.")

    def gold_customer_stats(**context):
        hook = PostgresHook(postgres_conn_id='postgres_dw')
        silver_schema = Variable.get('silver_schema')
        gold_schema = Variable.get('gold_schema')
        execution_date = context['ds']
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {gold_schema}.customer_stats WHERE DATE(aggregated_at) = %s", (execution_date,))

        cursor.execute(f"""
            INSERT INTO {gold_schema}.customer_stats
            (customer_type, total_transactions, total_revenue, avg_ticket)
            SELECT
                customer_type,
                COUNT(*) AS total_transactions,
                SUM(gross_revenue) AS total_revenue,
                ROUND(AVG(gross_revenue), 2) AS avg_ticket
            FROM (
                SELECT
                    gross_revenue,
                    CASE WHEN customer_id = 'ANONYMOUS' THEN 'anonymous' ELSE 'identified' END AS customer_type
                FROM {silver_schema}.transactions
                WHERE transaction_type = 'sale'
            ) subq
            GROUP BY customer_type
        """)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Gold customer_stats generado.")

    def gold_product_description_issues(**context):
        hook = PostgresHook(postgres_conn_id='postgres_dw')
        silver_schema = Variable.get('silver_schema')
        gold_schema = Variable.get('gold_schema')
        execution_date = context['ds']
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {gold_schema}.product_description_issues WHERE DATE(aggregated_at) = %s", (execution_date,))

        cursor.execute(f"""
            INSERT INTO {gold_schema}.product_description_issues
            (stock_code, description_count, canonical_name)
            SELECT
                stock_code,
                COUNT(DISTINCT description) AS description_count,
                MAX(description) AS canonical_name
            FROM {silver_schema}.transactions
            GROUP BY stock_code
            HAVING COUNT(DISTINCT description) > 1
            ORDER BY description_count DESC
        """)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Gold product_description_issues generado.")

    t_check = PythonOperator(
        task_id='check_silver_success',
        python_callable=check_silver_success
    )

    t_monthly = PythonOperator(
        task_id='gold_monthly_sales',
        python_callable=gold_monthly_sales
    )

    t_products = PythonOperator(
        task_id='gold_product_stats',
        python_callable=gold_product_stats
    )

    t_countries = PythonOperator(
        task_id='gold_country_stats',
        python_callable=gold_country_stats
    )

    t_customers = PythonOperator(
        task_id='gold_customer_stats',
        python_callable=gold_customer_stats
    )

    t_descriptions = PythonOperator(
        task_id='gold_product_description_issues',
        python_callable=gold_product_description_issues
    )

    t_check >> [t_monthly, t_products, t_countries, t_customers, t_descriptions]