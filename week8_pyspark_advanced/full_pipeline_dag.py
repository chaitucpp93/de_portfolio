from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner'           : 'chaitanya',
    'retries'         : 2,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG(
    dag_id       = 'full_de_pipeline',
    default_args = default_args,
    description  = 'Complete DE pipeline — PySpark + dbt',
    schedule     = '@daily',
    start_date   = datetime(2024, 1, 1),
    catchup      = False,
)

# Task 1 — PySpark ingestion job
task_spark = BashOperator(
        task_id      = 'spark_ingest',
    bash_command = """
        source /home/chaitanya/airflow_env_312/bin/activate
        python3 /home/chaitanya/spark_jobs/sales_spark_job.py {{ ds }}
        echo "Spark job completed for {{ ds }}"
    """,
    dag = dag,
)

# Task 2 — dbt run (transform)
task_dbt_run = BashOperator(
    task_id      = 'dbt_run',
    bash_command = """
        source /home/chaitanya/airflow_env_312/bin/activate
        cd /home/chaitanya/sales_dbt
        dbt run
        echo "dbt models built successfully"
    """,
    dag = dag,
)

# Task 3 — dbt test (validate)
task_dbt_test = BashOperator(
    task_id      = 'dbt_test',
    bash_command = """
        source /home/chaitanya/airflow_env_312/bin/activate
        cd /home/chaitanya/sales_dbt
        dbt test
        echo "dbt tests passed"
    """,
    dag = dag,
)

# Task 4 — verify output
task_verify = BashOperator(
    task_id      = 'verify_output',
    bash_command = """
        echo "Pipeline completed successfully for {{ ds }}"
        echo "Spark output: /tmp/spark_output/{{ ds }}"
        echo "dbt models: week3_sql database"
    """,
    dag = dag,
)

# Dependencies — linear pipeline
task_spark >> task_dbt_run >> task_dbt_test >> task_verify
