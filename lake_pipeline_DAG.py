#import RedditAPIClass

import RedditAPIClass
from SpotifyAPIClass import *
from ormtables import *
from dwlrds import *
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime
from lakeorchestra import *
import configparser

##subreddit_list = ["theseareouralbums", "shareyourmusic", "acousticoriginals", "composer"]
#r = redditGetter(subreddit_list)

def updatelinks():
    c = conductor()
    c.updateredditorc()

with DAG(dag_id="lake_pipeline_DAG",
         start_date=datetime(2021,1,1),
         schedule="@hourly",
         catchup=False) as dag:

    fetch_reddit = PythonOperator(
        task_id='fetch_reddit_data',
        python_callable=RedditAPIClass.mainRedditGet)

    update_orchestration_table = PythonOperator(
        task_id='update_orchestration_table',
        python_callable=updatelinks)

    fetch_spotify_tracks = PythonOperator(
        task_id='fetch_new_spotify_tracks',
        python_callable=spotify_track_dag)

    fetch_spotify_artists = PythonOperator(
        task_id='fetch_new_spotify_artists',
        python_callable=spotify_artist_dag)


fetch_reddit >> update_orchestration_table >> fetch_spotify_tracks >> fetch_spotify_artists



#%%
