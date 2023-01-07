from YouTubeAPIClass import *
from ormtables import *
from dwlrds import *
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import date
from lakeorchestra import *

# YouTube Statistics DAG is scheduled to run once a day at 5 AM to pull the newest statistics of our videos which we
# have already pulled the metadata for with the YouTubeDAG. It is chosen to run only once every morning to not use up
# the quota so much and to have a daily picture of the development of the stats of the videos.

with DAG(dag_id="youtube_statistics_DAG",
         start_date=datetime(2022, 1, 1),
         schedule="0 5 * * *",
         catchup=False) as dag:

    update_youtube_videos_statistics = PythonOperator(
        task_id='update_youtube_videos_statistics',
        python_callable=youtube_video_statistics_DAG)

    update_youtube_channel_statistics = PythonOperator(
        task_id='update_youtube_channel_statistics',
        python_callable=youtube_channel_statistics_DAG)

update_youtube_videos_statistics >> update_youtube_channel_statistics
