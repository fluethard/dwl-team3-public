import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import pandas as pd
import os
import psycopg2
from dwlrds import *
from lakeorchestra import *
import configparser
import re
from youtube_title_parse import get_artist_title


# Class to handle all Spotify API calls
# Path: dwl-team3/SpotifyAPIClass.py

class SpotifyData:

    def __init__(self):
        #self.reddit_csv = reddit_csv
        self.links_df = []
        self.spotify_links_df = []
        self.sp = []
        self.spotify_extracted_df = []
        self.artistlist = []
        self.config = configparser.ConfigParser()
        self.dbConn = rdsSession('dwl_lake')
        self.orclist = []
        self.orcdict = {}
        self.artist_df = []
        self.path = '/home/ubuntu/airflow/dags/dwl-team3/'
        self.df = pd.DataFrame
        pass

    def authenticate_spotify(self):
        # Authenticate Spotify API using OAuth2
        self.config.read(self.path + 'my_config.ini')
        self.config.sections()

        os.environ['SPOTIPY_CLIENT_ID'] = self.config['spotify']['client_id']
        os.environ['SPOTIPY_CLIENT_SECRET'] = self.config['spotify']['client_secret']
        os.environ['SPOTIPY_REDIRECT_URI'] = 'http://127.0.0.1:9090'

        scope = "user-library-read"

        client_id = self.config['spotify']['client_id']
        client_secret = self.config['spotify']['client_secret']

        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager, requests_timeout=30, retries=10)


    def get_orchestration(self):
        # Get new entries from orchestration table
        # Instantiate Conductor class from lakeorchestra.py
        # called in read_reddit()
        orc = conductor()
        orc.spotifyorcget() # Get new entries from orchestration table
        self.orclist = orc.linklist
        #print(self.orclist)




    # def read_reddit(self):
    #     ***DEPRECATED***
        # Ingest csv from Reddit. This will be switched to reading from a table
        #self.links_df = pd.read_csv(self.reddit_csv)

    def read_reddit(self):
        # Read new links from redditlinks table
        # Append new links to self.links_df for further processing in transform_reddit_data()
        # Method dependencies: get_orchestration()
        try:
            self.config.read(self.path + 'my_config.ini')
            self.config.sections()
            connection = psycopg2.connect(user=self.config['lake']['user'],
                                          password=self.config['lake']['password'],
                                          host=self.config['lake']['host'],
                                          port=self.config['lake']['port'],
                                          database=self.config['lake']['database'])
            cursor = connection.cursor()

            #postgres_reddit_lookup = """SELECT album_artist_id FROM spotify_raw WHERE processed = 0 """

            #cursor.execute(postgres_reddit_lookup)
            self.get_orchestration()
            self.orctuple = tuple(self.orclist)
            #(self.orctuple)

            self.links_df = pd.read_sql_query("SELECT permalink, url FROM redditlinks WHERE permalink IN {}".format(self.orctuple), connection)
            print(self.links_df.tail())


        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                #print("PostgreSQL connection is closed")

    def transform_reddit_data(self):
        # Method to transform data from Reddit using regex to extract Spotify links
        # Uses self.links_df as input
        # Filter out only Spotify Track links, Album links might be processed at a later point
        self.spotify_links_df = self.links_df[self.links_df.url.str.contains('.spotify.')]
        self.spotify_extracted_df = self.spotify_links_df.copy()
        self.spotify_extracted_df['trackuri'] = self.spotify_links_df['url'].str.extract("https://open.spotify.com/track/(\w+)")
        self.spotify_extracted_df = self.spotify_extracted_df.dropna()
        self.spotify_extracted_df = self.spotify_extracted_df.reset_index(level=None, inplace=False, drop=True )
        print(self.spotify_extracted_df)

    def tryextract(self, entry):
        # Method to extract data for records that may be inconsistent
        try:
            return entry
        except Exception:
            return '0'
        finally:
            pass


    def get_spotify_track_data(self, file_in, reddit_permalink):
        # Process data extracted from Reddit, make Spotify API calls, process json file, and write Spotify Track Data to DB
        # file_in = single json file from Spotify API call
        # reddit_permalink = single permalink from Reddit
        # called by process_track_data()
        self.authenticate_spotify()

        #file_in = self.sp.track('4rBDwVatn86sMwFiSv5a89', market=None)

        try:
            # print(file_in)  # verbose for debugging
            # print(file_in['external_ids']) # Verbose for debugging

            # Define Object for use with sqlAlchemy ORM, unpack Spotify API json file (file_in)
            object = spotifyTracks(
                album_artist = file_in['album']['artists'][0]['external_urls']['spotify'],
                album_artist_href = file_in['album']['artists'][0]['href'],
                album_artist_id = file_in['album']['artists'][0]['id'],
                album_artist_name = file_in['album']['artists'][0]['name'],
                album_artist_type = file_in['album']['artists'][0]['type'],
                album_artist_uri = file_in['album']['artists'][0]['uri'],

                album_available_markets = file_in['album']['available_markets'],
                album_external_urls = file_in['album']['external_urls']['spotify'],
                album_href = file_in['album']['href'],
                album_id = file_in['album']['id'],
                album_images = file_in['album']['images'][1]['url'],
                album_name = file_in['album']['name'],
                album_release_date = file_in['album']['release_date'],
                album_release_date_precision = file_in['album']['release_date_precision'],
                album_total_tracks = file_in['album']['total_tracks'],
                album_type = file_in['album']['type'],


                artist_external_url = file_in['artists'][0]['external_urls']['spotify'],
                artist_href = file_in['artists'][0]['href'],
                artist_id = file_in['artists'][0]['id'],
                artist_name = file_in['artists'][0]['name'],
                artist_type = file_in['artists'][0]['type'],
                artist_uri = file_in['artists'][0]['uri'],

                track_available_markets = file_in['available_markets'],
                track_disc_number = file_in['disc_number'],
                track_duration = file_in['duration_ms'],
                track_is_explicit = file_in['explicit'],
                track_isrc = str(file_in['external_ids']),
                track_external_url = file_in['external_urls']['spotify'],
                track_href = file_in['href'],
                track_id = file_in['id'],
                track_is_local = file_in['is_local'],
                track_name = file_in['name'],
                track_popularity = file_in['popularity'],
                track_preview_url = file_in['preview_url'],
                track_number = file_in['track_number'],
                track_type = file_in['type'],
                track_uri = file_in['uri']
            )

            orchestration_object = lakeOrchestra(
                permalink = reddit_permalink,
                spotify_track_id = file_in['id'],
                #spotify_artist_id = file_in['artists'][0]['id']
            )

            self.dbConn.update_track_id(orchestration_object)
            self.dbConn.add_to_table(object)
        except exc.SQLAlchemyError as e:
            print(e)
            pass


    def process_track_data(self):
        # Top-level Method to process Spotify Track Data
        # Uses self.spotify_extracted_df as input
        # Calls get_spotify_track_data() to write Spotify Track Data to DB

        self.dbConn.open_session()
        for i in range(len(self.spotify_extracted_df)):
            try:
                if self.spotify_extracted_df['trackuri'][i] != 'nan':
                    response = self.sp.track((self.spotify_extracted_df['trackuri'][i]), market=None)
                    reddit_permalink = self.spotify_extracted_df['permalink'][i]
                    #print(response)
                    self.get_spotify_track_data(response, reddit_permalink)
                else:
                    pass
            except:
                pass
        self.dbConn.commit_session()
        self.dbConn.close_session()

    def process_track_data_crossref(self, uri):
        self.authenticate_spotify()
        trackresponse = self.sp.track(uri, market=None)
        self.get_spotify_crossref_track_data(trackresponse)

    def get_spotify_crossref_track_data(self, file_in):
        self.authenticate_spotify()

        #file_in = self.sp.track('4rBDwVatn86sMwFiSv5a89', market=None)

        try:
            # print(file_in)  # verbose for debugging
            # print(file_in['external_ids']) # Verbose for debugging
            ts = datetime.now()

            # Define Object for use with sqlAlchemy ORM, unpack Spotify API json file (file_in)
            object = spotifyTracks(
                album_artist = file_in['album']['artists'][0]['external_urls']['spotify'],
                album_artist_href = file_in['album']['artists'][0]['href'],
                album_artist_id = file_in['album']['artists'][0]['id'],
                album_artist_name = file_in['album']['artists'][0]['name'],
                album_artist_type = file_in['album']['artists'][0]['type'],
                album_artist_uri = file_in['album']['artists'][0]['uri'],

                album_available_markets = file_in['album']['available_markets'],
                album_external_urls = file_in['album']['external_urls']['spotify'],
                album_href = file_in['album']['href'],
                album_id = file_in['album']['id'],
                album_images = file_in['album']['images'][1]['url'],
                album_name = file_in['album']['name'],
                album_release_date = file_in['album']['release_date'],
                album_release_date_precision = file_in['album']['release_date_precision'],
                album_total_tracks = file_in['album']['total_tracks'],
                album_type = file_in['album']['type'],


                artist_external_url = file_in['artists'][0]['external_urls']['spotify'],
                artist_href = file_in['artists'][0]['href'],
                artist_id = file_in['artists'][0]['id'],
                artist_name = file_in['artists'][0]['name'],
                artist_type = file_in['artists'][0]['type'],
                artist_uri = file_in['artists'][0]['uri'],

                track_available_markets = file_in['available_markets'],
                track_disc_number = file_in['disc_number'],
                track_duration = file_in['duration_ms'],
                track_is_explicit = file_in['explicit'],
                track_isrc = str(file_in['external_ids']),
                track_external_url = file_in['external_urls']['spotify'],
                track_href = file_in['href'],
                track_id = file_in['id'],
                track_is_local = file_in['is_local'],
                track_name = file_in['name'],
                track_popularity = file_in['popularity'],
                track_preview_url = file_in['preview_url'],
                track_number = file_in['track_number'],
                track_type = file_in['type'],
                track_uri = file_in['uri']
            )

            self.dbConn.add_to_table(object)
        except exc.SQLAlchemyError as e:
            print(e)
            pass


    # METHOD DEPRECATED
    #def write_to_database(self):
    #     spotify_insert_query = """INSERT INTO spotify_raw (album_artist, album_artist_href, album_artist_id, album_artist_name, album_artist_type, album_artist_uri, album_available_markets)"""
    #     record_to_insert = (album_artist, album_artist_href, album_artist_id, album_artist_name, album_artist_type, album_artist_uri, album_available_markets)
    #     pass
    # METHOD DEPRECATED

    def get_unprocessed_artist_list(self):
        # Top-level Method to get list of artists that have not been processed from orchestration table
        # produces self.artist_df as output for further use in process_artist_data()
        try:
            self.config.read(self.path + 'my_config.ini')
            self.config.sections()
            connection = psycopg2.connect(user=self.config['lake']['user'],
                                          password=self.config['lake']['password'],
                                          host=self.config['lake']['host'],
                                          port=self.config['lake']['port'],
                                          database=self.config['lake']['database'])

            cursor = connection.cursor()

            postgres_artist_lookup = """SELECT lakeorchestra.permalink, spotifytracks.artist_id FROM lakeorchestra LEFT JOIN spotifytracks ON lakeorchestra.spotify_track_id = spotifytracks.track_id WHERE lakeorchestra.spotify_track_id IS NOT NULL AND lakeorchestra.spotify_artist_id IS NULL"""



### Does this Lookup even work when you already write the artist_id into the orchestration Table when pulling the track data?
### Is it really usefull or does an update of the orchestration table only with the track data be enough?



            #postgres_artist_lookup = """SELECT album_artist_id FROM spotifyTracks"""

            #cursor.execute(postgres_artist_lookup)
            #artistidlist = cursor.fetchall()
            # self.artistlist = []
            #for i in range(len(artistidlist)):
            #    self.artistlist.append(artistidlist[i][0])
            #print(artistidlist)
            #print(self.artistlist)

            self.artist_df = pd.read_sql_query(postgres_artist_lookup, connection)
            #(self.artist_df)


        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        finally:
        # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                #print("PostgreSQL connection is closed")




    def get_spotify_artist_data(self, json_in, reddit_permalink):
        # Second-level Method to process Spotify Artist Data
        # called by process_artist_data()
        self.authenticate_spotify()

        try:
            object = spotifyArtists(
                artist_id = json_in['id'],
                followers = json_in['followers']['total'],
                genres = json_in['genres'],
                artist_name = json_in['name'],
                artist_uri = json_in['uri'],
                artist_popularity = json_in['popularity'],
            )

            orchestration_object = lakeOrchestra(
                permalink = reddit_permalink,
                spotify_artist_id = json_in['id'],
                spotify_updated = datetime.now()
            )

            self.dbConn.update_artist_id(orchestration_object)
            self.dbConn.add_to_table(object)



            #postgres_insert_query = """INSERT INTO spotify_artists_raw (followers, artist_id, artist_name, genres, popularity, artist_uri) VALUES (%s, %s, %s, %s, %s, %s)"""
            #record_to_insert = (followers, artist_id, artist_name, genres, artist_popularity, artist_uri)

            #cursor.execute(postgres_insert_query, record_to_insert)
            #connection.commit()
            #count = cursor.rowcount
            #print(count, "Record inserted successfully into spotify_artists_raw table")


        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)
            pass


    def process_artist_data(self):
        # Top-level Method to process Spotify Artist Data
        # Uses self.artist_df as input
        # calls get_spotify_artist_data() to write Spotify Artist Data to DB
        self.dbConn.open_session()
        for i in range(len(self.artist_df)):
            try:
                response = self.sp.artist(self.artist_df['artist_id'][i])
                reddit_permalink = self.artist_df['permalink'][i]
                print(response)
                self.get_spotify_artist_data(response, reddit_permalink)
            except:
                continue

        self.dbConn.commit_session()
        self.dbConn.close_session()


    def get_spotify_track_popularity(self):
        # Top-level Method to get Spotify Track Popularity
        # Uses bulk (50 per request) Spotify API call to get track popularity

        self.authenticate_spotify()
        self.dbConn.open_session()
        spotify_tracklist = self.dbConn.lake_query("""SELECT track_id FROM spotifytracks""")

        # create chunked tracklist for batch API call
        chunked_tracklist = [spotify_tracklist[i:i + 50] for i in range(0, len(spotify_tracklist), 50)]
        #print(chunked_tracklist)

        # iterate through chunked tracklist and call API
        for i in range(len(chunked_tracklist)):
            response = self.sp.tracks(chunked_tracklist[i], market=None)
            #print(response)
            # iterate through batch response and write to DB
            for j in range(len(response['tracks'])):
                object = popularityMetrics(
                    object_id = response['tracks'][j]['id'],
                    service = 'spotify',
                    type = 'track_popularity',
                    value_count = response['tracks'][j]['popularity'],
                )

                self.dbConn.add_to_table(object)
            self.dbConn.commit_session()
               # self.dbConn.update_track_popularity(track_id, track_popularity)
            #self.dbConn.commit_session()

        # for i in range(len(spotify_tracklist)):
        #     try:
        #         response = self.sp.track(spotify_tracklist[i])
        #         print(response)
        #         object = popularityMetrics(
        #             object_id = response['id'],
        #             service = 'spotify',
        #             type = 'track_popularity',
        #             value_count = response['popularity']
        #         )
        #         self.dbConn.add_to_table(object)
        #     except (Exception, psycopg2.Error) as error:
        #         print("Failed to fetch", error)
        #
        # self.dbConn.commit_session()

    def get_spotify_artist_popularity(self):
        # Top-level Method to get Spotify Artist Popularity
        # Uses bulk (50 per request) Spotify API call to get artist popularity
        self.authenticate_spotify()
        self.dbConn.open_session()
        spotify_artistlist = self.dbConn.lake_query("""SELECT artist_id FROM spotifyartists""")

        chunked_artistlist = [spotify_artistlist[i:i + 50] for i in range(0, len(spotify_artistlist), 50)]

        for i in range(len(chunked_artistlist)):
            response = self.sp.artists(chunked_artistlist[i])
            #print(response)
            for j in range(len(response['artists'])):
                object = popularityMetrics(
                    object_id = response['artists'][j]['id'],
                    service = 'spotify',
                    type = 'artist_popularity',
                    value_count = response['artists'][j]['popularity'],
                    followers = response['artists'][j]['followers']['total']
                )

                self.dbConn.add_to_table(object)
            self.dbConn.commit_session()

        # for i in range(len(spotify_artistlist)):
        #     try:
        #         response = self.sp.artist(spotify_artistlist[i])
        #         print(response)
        #         object = popularityMetrics(
        #             object_id = response['id'],
        #             service = 'spotify',
        #             type = 'artist_popularity',
        #             value_count = response['popularity'],
        #             followers = response['followers']['total']
        #         )
        #         self.dbConn.add_to_table(object)
        #     except (Exception, psycopg2.Error) as error:
        #         print("Failed to fetch", error)
        # self.dbConn.commit_session()

    def get_new_videos(self):

        self.new_yt_video_list = self.dbConn.lake_query2("""SELECT permalink, youtube_video_id FROM lakeorchestra WHERE spotify_track_id IS NULL AND youtube_video_id IS NOT NULL AND crossref_done = False AND yt_cleaning_status = 'done' LIMIT 2000 """)
        #print(self.new_yt_video_list)


    def crossref_yt_to_spotify(self):
        for i in range(len(self.new_yt_video_list)):
            list_permalink = self.new_yt_video_list[i][0]
            #print(list_permalink)
            print(self.new_yt_video_list[i][1])
            record = self.dbConn.lake_query2("""SELECT DISTINCT(video_id), artist, song_title FROM public."YouTubeCrossRef" WHERE video_id = %s""", (self.new_yt_video_list[i][1],))
            #print(record)
            self.dbConn.open_session()
            for row in record:
                video_id = row[0]
                artist = row[1]
                song_title = row[2]
                print(video_id, artist, song_title)



                try:
                    q = 'track:' + song_title + ' ' + 'artist:' + artist
                    #print(q)
                    #q = urllib.parse.quote(rawquery)
                    response = self.sp.search(q, limit=1, type='track')
                    spotify_track_id = response['tracks']['items'][0]['id']

                    self.process_track_data_crossref(spotify_track_id)

                    orchestration_object = lakeOrchestra(
                        permalink = list_permalink,
                        youtube_video_id = video_id,
                        spotify_track_id = spotify_track_id,
                        crossref_done = True,
                        crossref_success = True,
                        all_tasks_done = True
                    )
                    self.dbConn.update_track_crossref_spotify(orchestration_object)

                except:
                    orchestration_object = lakeOrchestra(
                        permalink = list_permalink,
                        youtube_video_id = video_id,
                        spotify_track_id = None,
                        crossref_done = True,
                        crossref_success = False,
                        all_tasks_done = True
                    )
                    self.dbConn.update_track_crossref_spotify(orchestration_object)

            self.dbConn.commit_session()
            self.dbConn.close_session()








def spotify_main():
    s = SpotifyData()
    s.authenticate_spotify()
    s.read_reddit()
    s.transform_reddit_data()
    s.process_track_data()
    s.get_unprocessed_artist_list()
    s.process_artist_data()

def spotify_track_dag():
    s = SpotifyData()
    s.authenticate_spotify()
    s.read_reddit()
    s.transform_reddit_data()
    s.process_track_data()

def spotify_artist_dag():
    s = SpotifyData()
    s.authenticate_spotify()
    s.get_unprocessed_artist_list()
    s.process_artist_data()

def spotify_track_popularity_dag():
    s = SpotifyData()
    s.authenticate_spotify()
    s.get_spotify_track_popularity()

def spotify_artist_popularity_dag():
    s = SpotifyData()
    s.authenticate_spotify()
    s.get_spotify_artist_popularity()

def crossref_dag():
    s = SpotifyData()
    s.authenticate_spotify()
    s.get_new_videos()
    s.crossref_yt_to_spotify()









#test = SpotifyData()
#spotify_track_dag()
#test.authenticate_spotify()
#test.read_reddit()
#test.transform_reddit_data()
#test.process_track_data()
#test.get_unprocessed_artist_list()
#test.process_artist_data()
#test.get_spotify_track_popularity()
#test.get_spotify_artist_popularity()
#spotify_track_popularity_dag()
#spotify_artist_popularity_dag()
#sdf
#spotify_track_dag()

#%%
