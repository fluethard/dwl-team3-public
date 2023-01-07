from ormtables import *
from dwlrds import *
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime
from lakeorchestra import *
from YouTubeAPIClass import *

# DAG was specifically scheduled at 10 and 10 PM due to the quota reset of the YouTube API at 9 AM!
# Therefore, we can get the new cross-references twice a day and in case the quota is already used up in the second run,
# the quota is refilled for the next run in 12 hours which then should make it work again without any problem

with DAG(dag_id="youtube_crossref_DAG",
         start_date=datetime(2021, 1, 1),
         schedule="0 10,22 * * *",
         catchup=False) as dag:

    search_crossrefs_in_youtube = PythonOperator(
        task_id='search_crossrefs_in_youtube',
        python_callable=youtube_cross_ref_DAG)

search_crossrefs_in_youtube
