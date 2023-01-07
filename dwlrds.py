from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import force_auto_coercion
from sqlalchemy import exc
from ormtables import *
import psycopg2
import configparser
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.types import Integer, Text, String, Date, DateTime, Float, Boolean
import sqlalchemy

# Class for interacting with the database
# Class-based approach chosen to allow working with sqlalchemy sessions


force_auto_coercion()


class rdsSession:

    def __init__(self, db):
        self.db = db
        self.config = configparser.ConfigParser()
        pass

    def open_session(self):
        # Create engine and open session to DB


        self.config.read('/home/ubuntu/airflow/dags/dwl-team3/my_config.ini')
        self.config.sections()
        conn_string = self.config['lake_string']['conn_string']
        print(conn_string)

        try:
            engine = create_engine(conn_string)
            Session = sessionmaker(bind=engine)

            self.e = engine

            self.s = Session()
            print('Session opened')

        except Exception as e:
            print('Failed to open session. connection attempt failed')
            print(e)

    def add_to_table(self, object):
        # General method for adding entries to the database
        # IMPORTANT: Only adding new entries, not updating existing ones
        try:
            self.s.merge(object)  # Merge chosen to avoid duplicate entries and ensure idempotency
            print("Object added to DB")
        except:
            print('addToTable error, aborting...')

    def update_track_id(self, object):
        # Method for updating track id in lakeOrchestra table
        # Called by spotifyApiClass get_spotify_track_data() method
        try:
            x = self.s.query(lakeOrchestra).get(object.permalink)
            x.spotify_track_id = object.spotify_track_id
            print("Object updated")
        except:
            print('updateObject error, aborting...')

    def update_artist_id(self, object):
        # Method for updating artist id in lakeOrchestra table
        # Called by spotifyApiClass get_spotify_artist_data() method
        try:
            x = self.s.query(lakeOrchestra).get(object.permalink)
            x.spotify_artist_id = object.spotify_artist_id
            x.spotify_updated = object.spotify_updated
            print("Object updated")
        except:
            print('updateObject error, aborting...')

    def update_track_crossref_spotify(self, object):
        try:
            x = self.s.query(lakeOrchestra).get(object.permalink)
            x.spotify_track_id = object.spotify_track_id
            x.crossref_done = object.crossref_done
            x.crossref_success = object.crossref_success
            x.all_tasks_done = object.all_tasks_done
            # print("Object updated")
        except:
            print('updateObject error, aborting...')

    def update_track_crossref_youtube(self, object):
        try:
            x = self.s.query(lakeOrchestra).get(object.permalink)
            x.youtube_video_id = object.youtube_video_id
            x.crossref_done = object.crossref_done
            x.crossref_success = object.crossref_success
            x.all_tasks_done = object.all_tasks_done
            print("Object updated")
        except Exception as e:
            print('updateObject error, aborting...')
            print(e)

    def update_clean_yt(self, object):
        try:
            x = self.s.query(lakeOrchestra).get(object.permalink)
            x.yt_cleaning_status = object.yt_cleaning_status
            # print("Object updated")
        except Exception as e:
            print('updateObject error, aborting...')
            print(e)

    # ***DEPRECATED***
    # def extract_url(self):
    #     urllist = []
    #     for url in self.s.query(redditlinks.url):
    #         urllist.append(url)
    #
    #     return urllist

    def update_youtube_video_ids(self, object):
        # Method for updating track id in lakeOrchestra table
        # Called by spotifyApiClass get_spotify_track_data() method
        try:
            x = self.s.query(lakeOrchestra).get(object.permalink)
            x.youtube_video_id = object.youtube_video_id
            x.youtube_channel_id = object.youtube_channel_id
            print("Object updated")
        except:
            print('updateObject error, aborting...')

    def update_youtube_channel_ids(self, object):
        # Method for updating track id in lakeOrchestra table
        # Called by spotifyApiClass get_spotify_track_data() method
        try:
            x = self.s.query(lakeOrchestra).get(object.permalink)
            x.youtube_channel_id = object.youtube_channel_id
            x.youtube_updated = object.youtube_updated
            print("Object updated")
        except:
            print('updateObject error, aborting...')

    def commit_session(self):
        # Commit changes to DB
        # Called by anything that commits changes to the session
        try:
            print('Initialize commit')
            self.s.commit()
            print('Session commited to DB')
        except exc.SQLAlchemyError as e:
            print(e)

    def close_session(self):
        self.s.close
        print('Session closed')

    def create_tables(self):
        # Initial method for creating tables in the database based on definitions in ormtables.py
        Base.metadata.create_all(self.e)
        print('Tables created')

    def lake_query(self, postgres_lookup):
        # Method for querying the lakeOrchestra table
        # Simplified way of querying the table with raw sql
        try:
            self.config = configparser.ConfigParser()
            self.config.read('/home/ubuntu/airflow/dags/dwl-team3/my_config.ini')
            self.config.sections()

            connection = psycopg2.connect(user=self.config['lake']['user'],
                                          password=self.config['lake']['password'],
                                          host=self.config['lake']['host'],
                                          port=self.config['lake']['port'],
                                          database=self.config['lake']['database'])

            cursor = connection.cursor()

            cursor.execute(postgres_lookup)
            result = cursor.fetchall()
            linklist = []
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            for i in range(len(result)):
                linklist.append(result[i][0])
            print(linklist)
            return linklist

        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        # finally:
        #     # closing database connection.
        #     if connection:
        #         cursor.close()
        #         connection.close()
        #         print("PostgreSQL connection is closed")
        #     else:
        #         print("PostgreSQL connection not established")

    def bulk_insert_to_table(self, table, mapping):
        # General method for adding entries to the database
        # IMPORTANT: Only adding new entries, not updating existing ones
        try:
            self.s.bulk_insert_mappings(table,
                                        mapping)  # Merge chosen to avoid duplicate entries and ensure idempotency
            print("Objects added to DB")
        except Exception as e:
            print('addToTable error, aborting...')
            print(e)

    def add_all_to_table(self, objects):
        # General method for adding entries to the database
        # IMPORTANT: Only adding new entries, not updating existing ones
        try:
            self.s.add_all(objects)  # Merge chosen to avoid duplicate entries and ensure idempotency
            print("Objects added to DB")
        except Exception as e:
            print('addToTable error, aborting...')
            print(e)

    def lake_query2(self, postgres_lookup, args=None):
        # Method for querying the lakeOrchestra table
        # Simplified way of querying the table with raw sql
        try:
            self.config.read('/home/ubuntu/airflow/dags/dwl-team3/my_config.ini')
            self.config.sections()
            connection = psycopg2.connect(user=self.config['lake']['user'],
                                          password=self.config['lake']['password'],
                                          host=self.config['lake']['host'],
                                          port=self.config['lake']['port'],
                                          database=self.config['lake']['database'])
            cursor = connection.cursor()

            cursor.execute(postgres_lookup, args)
            result = cursor.fetchall()
            linklist = []
            # cursor.close()
            # connection.close()
            # print("PostgreSQL connection is closed")

            return result

        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                # print("PostgreSQL connection is closed")


#dbsession = rdsSession('dwl_lake')
#dbsession.open_session()
# dbsession.create_tables()
# dbsession.close_session()

# %%
