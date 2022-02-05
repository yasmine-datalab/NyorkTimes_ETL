from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# defaults args

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': '2021-12-05',
    'end_date': '2022-01-01',
    'directory': '~/Documents/holidays_challenge/Nytimes_scraper'
}
with DAG(
    'etl',
    default_args=default_args,
    description='',
    schedule_interval=timedelta(1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    task_cd = BashOperator(
        task_id = "cd",
        bash_command = "cd "+str(default_args['directory'])
    )
    task_ingestion = BashOperator(
        task_id='ingest',
        bash_command='python /src/sraper.py -k "football"',
    )
    task_ingestion.doc_md = dedent(
        """\
    #### Task Documentation
        Ingestion task.
        This task get articles on NYTIMES API and store unstructured
        data into DATALAKE

    """
    )
    task_etl = BashOperator(
        task_id='process',
        bash_command= 'python src/process.py -s '
                    + str(default_args['start_date'])
                    + ' -e ' + str(default_args['end_date']),
    )
    task_etl.doc_md = dedent(
        """\
    #### Task Documentation
        ETL task.
        This task unstructured data into DATALAKE
        treat, analyse and store into sructures data

    """
    )
    
    task_cd>>task_ingestion >> task_etl


