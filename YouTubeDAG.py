from ormtables import *
from dwlrds import *
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime
from lakeorchestra import *
from YouTubeAPIClass import *

# YouTube DAG scheduled to run every hour to just pull the new leads from Reddit and after this initial pull based on
# the videos also pull the channel data to the video accordingly. Reason for hourly was to always pull the new leads in
# low quantity and therefore have a smooth running DAG.

with DAG(dag_id="youtube_update_DAG",
         start_date=datetime(2021, 1, 1),
         schedule="@hourly",
         catchup=False) as dag:

    fetch_youtube_videos = PythonOperator(
        task_id='fetch_new_youtube_videos',
        python_callable=youtube_videos_DAG)

    fetch_youtube_channels = PythonOperator(
        task_id='fetch_new_youtube_channels',
        python_callable=youtube_channels_DAG)

fetch_youtube_videos >> fetch_youtube_channels
