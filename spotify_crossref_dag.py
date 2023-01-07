from SpotifyAPIClass import *
from ormtables import *
from dwlrds import *
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime
from lakeorchestra import *
from CrossRefYouTubeToSpotify2 import *

with DAG(dag_id="spotify_crossref_DAG",
         start_date=datetime(2021, 1, 1),
         schedule="30 * * * *",
         catchup=False) as dag:
    get_new_crossrefs = PythonOperator(
        task_id='get_new_crossrefs_dag',
        python_callable=yt_to_spotify_crossref_dag)

    crossref_to_spotify = PythonOperator(
        task_id='do_crossref',
        python_callable=crossref_dag)

get_new_crossrefs >> crossref_to_spotify
