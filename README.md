# DWL - Team 3 - README

### Title: Discovering future music stars using big data

### Project Aim: 
In our project we tried to identify music artists with the potential to break through in the near future. 
The approach we applied is to generate leads through Reddit Posts in specific Subreddits which are dedicated to new music or 
for artists to promote their own music. Based on these subreddit posts we get YouTube, Spotify and links to other platforms, which we then
use to find these tracks/artists on YouTube and Spotify. Our scope was specifically reduced only to the YouTube and Spotify due to the time 
restriction of this project. Based on these leads/links, we then request the data to these videos/tracks through the YouTube and Spotify APIs
respectively. Once initially pulled we also get the metadata of the artist and channel to the specific track in the subreddit. 

Furthermore, we also cross-referenced the videos/tracks which we initially found to see if we can find them on both platforms.
This has been done by a rule-based approach in which we defined several rules to preprocess the data nd then also
double-check the found videos/tracks to see if we get the right cross-reference.

All of the above described tasks have then been orchestrated with Apache Airflow for which we created several separate
scripts which all serve one specific DAG in Apache Airflow. 

### Technologies:
General: 

- Python 3.8
- Apache Airflow
- PostgreSQL on AWS RDS
- AWS Glue
- Tableau

Special Python Libraries:
(INCLUDE ALL LIBRARIES USED, conda set up env)

- googleapiclient.discovery (YouTube Data API)
- spotipy (Spotify API)
- praw (Python Reddit API Wrapper)
- sqlalchemy (Interaction with DB)
- psycopg2 (Interaction with DB)
- youtube_title_parse (Specific module to extract tile and artist from YouTube Video Title)

All of these specific libraries are often not installed and have to be installed to run this project. Due to this we
created the code to create a new virtual environment on anaconda which includes all libraries needed to run this project.

Just run each of the lines below and rename the "yourvenv" variable to your needs and then use this newly created environment
to run the project.
```
conda create -n yourvenv python=3.8
conda activate yourvenv
pip install --upgrade google-api-python-client spotipy pandas praw sqlalchemy sqlalchemy-utils psycopg2 youtube-title-parse configparser datetime requests airflow pendulum
```


### Manual (How to run the repo):

The project can be manually executed based on the different DAGs which were used to orchestrate the code in this project.

In a first step the repository has to be pulled to your local device which can be done either by GitHub Desktop if you are
a Windows user or by using the console if you use Mac OS/Linux.

After pulling all the code and already installed the needed libraries (see above) we can execute it as we have it in the DAG files.

1. Get the Reddit Posts

Open the __RedditAPIClass.py__ file and execute the __mainRedditGet()__ function. This will directly pull all new Reddit posts from the 15 defined 
subreddits we use in this project. These Reddit posts will then be added to the lakeorchestra table.

2. Get YouTubeVideo Data

In a next step we can pull the YouTube Video Data from the Reddit leads we got above. For this we first run the __youtube_videos_DAG()__ function as well as the 
__youtube_channels_DAG()__ function from the YouTubeAPIClass.py module. With these two function we get the dimensional data to all new leads which we acquired in the previous step.
After we updated the YouTubeVideo and YouTubeChannels table with all new leads we can run the __youtube_video_statistics_DAG()__ and the __youtube_channel_statistics_DAG()__ functions
to pull the statistics for all our Videos and Channels which we already have and just updated.

3. Get Spotify Data

In a next step we can pull the Spotify Data from the Reddit leads we got above. For this we first run the __spotify_tracks_dag()__ function as well as the __spotify_artists_dag()__ function from the SpotifyAPIClass.py module. With these two function we get the dimensional data to all new leads which we acquired in the previous step.
After we updated the spotifytracks and the spotifyartists table we can run the __spotify_track_popularity_dag()__ and the __spotify_artist_popularity_dag()__ functions to retrieve the daily popularity updates.
 

4. Get Cross-References from YouTube to Spotify and vice versa

In the final step to load all our data into our Data Lake we have to do the cross-referencing. First we do the Spotify to YouTube
Cross-Referencing. For this we have to run the __youtube_cross_ref_DAG()__ function from the YouTubeAPIClass.py module. This function will search for
the video which corresponds to the track in Spotify and directly update the cross-reference status in the lakeorchestra table as well
as pulling the dimension data to this video into the YouTubeVideos table. 

For the YouTube to Spotify Cross-Reference we need to first extract the artist name and track name from the new leads. To achieve this we first have to run the 
__yt_to_spotify_crossref_dag()__ function from the CrossRefYouTubeToSpotify2.py module. The artist name, track name and video id are then writen into the YouTubeCrossRef table on our data lake.
After this we can execute the __crossref_dag()__ function to search the tracks on Spotify with the artist and track name we extracted. With this function we search the song on 
Spotify and then write back the result into our Data Lake.


After these 4. Steps we have manually updated/pulled the data for our project.


### Files Description:

The files in this repo can be divided into the following three categories:
1. API Classes
2. DAG Files
3. Support Modules (SQL Alchemy's ORM Functions)

All files in the repository will be described below based on the three categories.

#### 1. API Classes:

YouTubeAPIClass.py:

This module includes the YouTubeData class which is used for all the interactions with the YouTube API V3. It has several functions
which serve three main purposes. These three parts, the YouTube Videos Functions, the YouTube Channels Functions and the YouTube Cross-Reference Functions, are specifically 
shown in the file with different parts (****** Part ******). Additionally, there are also some summarizing functions to sum up several functions to one, as well as the DAG functions
which are not part of the class anymore but made the setting up of the DAGs easier with just one function to execute.

SpotifyAPIClass.py:

This module invokes the SpotifyData class to perform functions pertaining to operations with Spotify Data.

The main functionality consists of X parts.

First, data is pulled internally from the data ingested from the RedditAPIClass into the data lake.  This data is then processed to extract Spotify URLs from the Reddit posts. 
These URLs are then used to pull data from the Spotify API. This data, in turn, is then written to the data lake and the orchestration table is updated.

A second set of functions queries the Spotify API only for track and artist popularity. These functions are used for daily tracking in the spotify_track_popularity_dag and spotify_artist_popularity_dag.

A third set of functions handles the spotify-side of the cross-referencing. These functions are used in the spotify_cross_ref_dag.

RedditAPIClass.py:

The RedditAPIClass.py with the redditGetter class represents the foundation of our project. It is used to pull the data from the Reddit API and to write it into our data lake.
Instantiating the class lets the user of this class specify a list of subreddits that should be investigated.
By default, the class is instantiated with a post limit of the 1000 most recent posts of a given subreddit. This is the maximum amount of posts supported by the API without recourse to pagination.
We found that this amount of posts is sufficient to get our project started as anything more would already tend to be outdated for our tracking purposes.
After the initial pull, we reduced the post limit to 50, but checked them on an hourly basis, to ensure timely tracking of song/artist recommendations.

CrossRefYouTubeToSpotify2.py:

This module instantiates the YouTubeCrossReference class to perform the preparatory data extraction and cleaning to cross reference the existing results from YouTube to Spotify.
As YouTube does not provide the same level of detail and data quality as Spotify in regards to separation of the track title and artist name, it is necessary to extract that information based on the video and channel titles.
The separateArtistTitle method first tries to do this task using the youtube_title_parse library. 
If this fails, it uses our custom logic (largely based on regex) to extract the artist and track name from the video title and channel name.

#### 2. DAG Files:

YouTubeDAG.py:

Main DAG file for the YouTube data. It orchestrates the pull of the dimensional data of the found or cross-referenced YouTube Videos
and is scheduled to run hourly. It first runs the youtube_video_DAG which pulls the dimensional data for all newly found or cross-referenced videos
and then secondly runs the youtube_channels_DAG which accesses the channel information for all newly acquired video dimensional data. Both functions
which are run in this DAG file are from the YouTubeAPIClass.py module.

youtube_statistics_DAG.py:

This DAG files orchestrates the pull of all YouTube Statistics data (views, comments, etc.) from the YouTube API. It is scheduled to run once a day, 
because the amount of data is getting more and more over the course of this project and the goal is to track daily differences in the statistics on the 
two platforms. The file runs the two functions youtube_video_statistics_DAG and youtube_channel_statistics_DAG from the YouTubeAPIClass.py module.

YouTubeCrossRefDAG.py:

The third YouTube DAG file is specifically for the Cross-Referencing. This DAG only automates the youtube_cross_ref_DAG because of the quota problems, which 
could make the DAG fail and therefore only make the Cross-Ref DAG fail and not the other YouTube DAGs. With this DAG the newly found Spotify Tracks get taken 
searched in YouTube to see if we can find the corresponding Video to the Track and then pull the data to the video.

lake_pipeline_DAG.py:

This DAG orchestrates the pull of Reddit data, updates the orchestration table, and then pulls the initial Spotify track and artist data for any found Spotify recommendations.
With the logic we had in mind, we recommend scheduling this DAG to run every hour.

spotify_popularity_DAG.py:

This DAG pulls the Spotify track and artist popularity data for the tracks and artists that are already in the data lake. 
This DAG is scheduled to run once a day.


spotify_crossref_dag.py:

This DAG orchestrates the cross-referencing of the YouTube to Spotify data. It first runs the yt_to_spotify_crossref_dag function from the CrossRefYouTubeToSpotify2.py module
Then it runs the crossref_dag function from the SpotifyAPIClass.py module. This DAG is scheduled to run once a day.




#### 3. Support Modules (SQL Alchemy's ORM Functions)

ormtables.py:

This module holds all metadata for the tables on our PostgreSQL data lake. It builds the basis for any write operation
into the DB based on the ORM function of SQL Alchemy. Each table builds an own class and can then be referenced in the 
API Classes to load the data directly into the table by using the table/class as an object in which we can assign certain 
variables/data to and load into the file by using other functions we defined in the next support module.


dwhormtables.py:

This module has the same purpose as the one described above, but this time it holds the metadata for the tables in our PostgreSQL 
data warehouse (dwh). It builds the basis for any write operation into the DB based on the ORM function of SQL Alchemy. Each table builds an own class and can then be referenced in the
API Classes to load the data directly into the table by using the table/class as an object in which we can assign certain
variables/data to and load into the file by using other functions we defined in the next support module.

dwlrds.py:

This module manages our database interactions with the data lake. It contains functions to read, write and update data.


dwhrds.py:

This module manages our database interactions with the data warehouse. It contains functions to read, write and update data.


lakeorchestra.py:

This module manages the core functionality of the orchestration table in the data lake.
As our primary source of data is Reddit, we need to keep track of the posts we have already processed.
The lakeorchestra functions take care of this task.


### Authors:
Dimitri Rossi (dimitri.rossi@stud.hslu.ch)

Fabian Lüthard (fabian.luethard@stud.hslu.ch)

Michael Seiler (michael.seiler@stud.hslu.ch)
