from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id = 'create_stocks_table_dag',
    start_date = datetime(2025, 2, 12),
    schedule = '@once',
    catchup = False
) as dag:
    create_stocks_table = SQLExecuteQueryOperator(
        task_id = 'create_stock_table',
        conn_id = 'postgres_localhost',
        sql= 'sql/create_stocks_table.sql',
    )


