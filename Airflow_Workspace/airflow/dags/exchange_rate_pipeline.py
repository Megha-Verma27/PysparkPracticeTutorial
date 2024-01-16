from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clean_data import clean_data


dag = DAG(
    'exchange_rate_etl',
    start_date=datetime(2023, 10, 1),
    end_date=datetime(2023, 12, 31),
    schedule_interval='0 22 * * *',
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=False
)


download_task = BashOperator(
    task_id= 'download_file',
    bash_command='curl -o xrate.csv https://data-api.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=csvdata',
    cwd='/tmp',
    dag = dag,
)

clean_data_task = PythonOperator(
    task_id = 'clean_data',
    python_callable = clean_data,
    dag = dag
)


send_email_task = EmailOperator(
    task_id = 'send_email',
    to = 'codewithz21@gmail.com',
    subject = 'Excahnge Rate Download - Successful',
    html_content='The Exchange Rate data has been successfully download, cleaned, and loaded.',
    dag = dag
)

download_task >> clean_data_task >> send_email_task
