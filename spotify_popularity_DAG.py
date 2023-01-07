import RedditAPIClass
from RedditAPIClass import *
from SpotifyAPIClass import *
from ormtables import *
from dwlrds import *
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import date
from lakeorchestra import *

#subreddit_list = ["theseareouralbums", "shareyourmusic", "acousticoriginals", "composer"]
#r = redditGetter(subreddit_list)


with DAG(dag_id="spotify_popularity_DAG",
         start_date=datetime(2022,1,1),
         schedule="0 4 * * *",
         catchup=False) as dag:

    update_spotify_track_popularity = PythonOperator(
        task_id='update_track_popularity',
        python_callable=spotify_track_popularity_dag)

    update_spotitfy_artist_popularity = PythonOperator(
        task_id='update_artist_popularity',
        python_callable=spotify_artist_popularity_dag)




update_spotify_track_popularity >> update_spotitfy_artist_popularity


