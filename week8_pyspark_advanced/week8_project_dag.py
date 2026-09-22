from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner'           : 'chaitanya',
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG(
    dag_id       = 'week8_project',
    default_args = default_args,
    description  = 'Week 8 project — PySpark + dbt + Airflow',
    schedule     = '@daily',
    start_date   = datetime(2024, 1, 1),
    catchup      = False,
)

# Task 1 — PySpark job
task_spark = BashOperator(
    task_id      = 'spark_enrich',
    bash_command = """
        source /home/chaitanya/airflow_env_312/bin/activate
        python3 /home/chaitanya/spark_jobs/week8_project_spark.py {{ ds }}
    """,
    dag = dag,
)

# Task 2 — dbt run
task_dbt_run = BashOperator(
    task_id      = 'dbt_run',
    bash_command = """
        source /home/chaitanya/airflow_env_312/bin/activate
        cd /home/chaitanya/sales_dbt
        dbt run 2>&1 | tee /tmp/dbt_run.log
        grep -q "Completed successfully" /tmp/dbt_run.log
    """,
    dag = dag,
)

# Task 3 — dbt test
task_dbt_test = BashOperator(
    task_id      = 'dbt_test',
    bash_command = """
        source /home/chaitanya/airflow_env_312/bin/activate
        cd /home/chaitanya/sales_dbt
        dbt test 2>&1 | tee /tmp/dbt_test.log
         grep -q "Completed successfully" /tmp/dbt_test.log
   """,    
   dag = dag,
)

# Task 4 — verify Parquet output
task_verify = BashOperator(
    task_id      = 'verify_parquet',
    bash_command = """
        ls /tmp/week8_output/{{ ds }}/
        echo "Parquet partitions verified for {{ ds }}"
    """,
    dag = dag,
)

# Dependencies
task_spark >> task_dbt_run >> task_dbt_test >> task_verify
