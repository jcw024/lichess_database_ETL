from datetime import timedelta, datetime
from textwrap import dedent
from download_games import download_file
from producer import start_producer
from airflow import DAG
from airflow.utils.state import State
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time
import os

DAG_PATH = os.path.realpath(__file__)
DAG_PATH = '/' + '/'.join(DAG_PATH.split('/')[1:-1]) + '/'

"""
def download_game(url):
    with open('log.txt', 'a') as f:
        f.write(f"{datetime.now()} downloading {url}\n")
    time.sleep(5)

def start_producer(url):
    with open('log.txt', 'a') as f:
        f.write(f"{datetime.now()} starting producer for {url}...\n")
    time.sleep(20)
"""
def delete_file(url):
    """takes file url and deletes file to save disk space. assumes data has been downloaded in ../data and sent by the producer"""
    with open(DAG_PATH + 'log.txt', 'a') as f:
        f.write(f"{datetime.now()} deleting {url}...\n")
    filepath = os.getcwd() + "/../data/" + url.split("/")[-1]
    os.remove(filepath)


default_args = {
        'owner': 'Joe',
        'depends_on_past':True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 10,
        'retry_delay':timedelta(seconds=30),
        'retry_exponential_backoff':True,
        #'max_retry_delay':timedelta(minutes=30),
        'execution_timeout':None
        }

with DAG(
    'lichess_ETL_pipeline_kafka',
    default_args=default_args,
    description='download game and send to kafka to write to db',
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(7),
    #end_date=datetime(2021,8,1),
    max_active_runs=1,
    catchup=False
    ) as dag2:

    with open(DAG_PATH + 'download_links.txt','r') as f:
        for line in f:
            url_list = f.read().splitlines()
            url_list = reversed(url_list)   #read the links from oldest to newest

    for i, url in enumerate(url_list):
        month = url.split("_")[-1].split(".")[0]
        download_task = PythonOperator(
                python_callable=download_file,
                task_id="downloader_" + month,
                op_args=(url,),
                op_kwargs={'years_to_download': [2013,2014,2015,2016,2017,2018]}
                )

        producer_task =  PythonOperator(
                python_callable=start_producer,
                task_id="producer_" + month,
                op_args=(url,)
                )
        delete_file_task = PythonOperator(
                python_callable=delete_file,
                task_id="deletion_" + month,
                op_args=(url,)
                )

        if i == 0:
            download_task >> producer_task >> delete_file_task
        else:
            prev_download_task >> download_task
            [prev_producer_task, download_task] >> producer_task >> delete_file_task
        if i >= 2:
            prev_prev_producer_task >> download_task    #downloads are usually faster than producer, we don't want
                                                        #downloaders to far outpace processing of files to minimize disk usage
        if i >= 1:
            prev_prev_producer_task = prev_producer_task
        prev_download_task = download_task
        prev_producer_task = producer_task
