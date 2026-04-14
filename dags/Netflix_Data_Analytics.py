from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

import sys
sys.path.append('/opt/airflow/dags')
from source_load.data_load import run_script
from alerting.callback_script import callback_function
from alerting.slack_alert import task_success_slack_alert

DBT_BIN     = 'dbt'
DBT_DIR     = '/opt/airflow/netflix_project'
DBT_PROFILE = 'netflix_project'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': callback_function
}

with DAG(
    dag_id='Netflix_Data_Analytics',
    default_args=default_args,
    description='ETL pipeline: S3 -> Snowflake -> dbt',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')

    credits_sensor = S3KeySensor(
        task_id='credits_rawfile_sensor',
        poke_interval=60 * 5,
        timeout=60 * 60 * 24 * 7,
        bucket_key='raw_files/credits.csv',
        wildcard_match=False,
        bucket_name='netflix-data-analytics-artvion',
        aws_conn_id='aws_default',
    )

    titles_sensor = S3KeySensor(
        task_id='titles_rawfile_sensor',
        poke_interval=60 * 5,
        timeout=60 * 60 * 24 * 7,
        bucket_key='raw_files/titles.csv',
        wildcard_match=False,
        bucket_name='netflix-data-analytics-artvion',
        aws_conn_id='aws_default',
    )

    load_data_snowflake = PythonOperator(
        task_id='Load_Data_Snowflake',
        python_callable=run_script,
    )

    run_stage_models = BashOperator(
        task_id='run_stage_models',
        bash_command=f'{DBT_BIN} run --select tag:STAGING '
                     f'--project-dir {DBT_DIR} --profile {DBT_PROFILE} --target dev',
    )

    run_fact_dim_models = BashOperator(
        task_id='run_fact_dim_models',
        bash_command=f'{DBT_BIN} run --select tag:DIMENSION,tag:FACT '
                     f'--project-dir {DBT_DIR} --profile {DBT_PROFILE} --target dev',
    )

    run_test_cases = BashOperator(
        task_id='run_test_cases',
        bash_command=f'{DBT_BIN} test '
                     f'--project-dir {DBT_DIR} --profile {DBT_PROFILE} --target dev',
    )

    slack_success = PythonOperator(
        task_id='slack_success_notification',
        python_callable=task_success_slack_alert,
    )
  

    end = EmptyOperator(task_id='end')

    start >> credits_sensor >> titles_sensor >> load_data_snowflake
    load_data_snowflake >> run_stage_models >> run_fact_dim_models >> run_test_cases
    run_test_cases >> slack_success >> end
