from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="house_price_pipeline",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Load raw CSV into Postgres
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command="python /opt/airflow/project/ml/data_prep.py --csv /opt/airflow/data/train.csv"
    )

    # Run dbt to to clean raw data
    run_dbt_stg = BashOperator(
        task_id="run_dbt_stg",
        bash_command="cd /opt/airflow/project/dbt && dbt run --select stg_house_prices"
    )

    # Run dbt to to create marts data
    run_dbt_clean = BashOperator(
        task_id="run_dbt_clean",
        bash_command="cd /opt/airflow/project/dbt && dbt run --select clean_house_prices"
    )

    # Train model
    train_model = BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/project/ml/train.py"
    )

    load_raw >> run_dbt_stg >> run_dbt_clean >> train_model
