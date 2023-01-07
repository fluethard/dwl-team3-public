from ormtables import *
from dwlrds import *
from datetime import datetime
import pandas as pd
import os
import psycopg2
import configparser
import pandas as pd

# This module and the conductor Class covers all interactions with the orchestration table


class conductor:
# class used for tasks done on dwl_lake orchestration table
    def __init__(self):
        self.dbConn = rdsSession('dwl_lake')
        self.config = configparser.ConfigParser()
        self.path = r'C:\Users\dimit\Desktop\my_config.ini'  # Path to specify the my_config.ini file (Local/Server)

    def redditorcget(self):
        # creates initial index for orchestration database
        # ONLY RUN ONCE to initialize the project
        try:
            self.config.read('/home/ubuntu/airflow/dags/dwl-team3/my_config.ini')
            self.config.sections()
            connection = psycopg2.connect(user=self.config['lake']['user'],
                                          password=self.config['lake']['password'],
                                          host=self.config['lake']['host'],
                                          port=self.config['lake']['port'],
                                          database=self.config['lake']['database'])
            cursor = connection.cursor()

            postgres_redditlinks_lookup = """SELECT permalink FROM redditlinks"""

            cursor.execute(postgres_redditlinks_lookup)
            result = cursor.fetchall()
            self.linklist = []

            for i in range(len(result)):
                self.linklist.append(result[i][0])
            print(self.linklist)


        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                #print("PostgreSQL connection is closed")

    def redditorcwrite(self):
        # write reddit permalinks to orchestration table
        # called by updateredditorc
        self.dbConn.open_session()

        for i in range(len(self.linklist)):
            try:
                object = lakeOrchestra(
                    permalink=self.linklist[i]
                    #created = datetime.datetime.now()
                )
                self.dbConn.add_to_table(object)
            except:
                print("error occurred")
                pass
        self.dbConn.commit_session()
        self.dbConn.close_session()

    def updateredditorc(self):
        # Check if new reddit links are already in the orchestration table by performing anti-join on redditlinks and lakeOrchestra
        #update orchestration table "lakeorchestra" with new reddit links
        query = self.dbConn.lake_query("""SELECT redditlinks.permalink FROM lakeOrchestra RIGHT JOIN redditlinks ON redditlinks.permalink = lakeOrchestra.permalink WHERE lakeOrchestra.permalink IS NULL""")
        self.linklist = query
        self.redditorcwrite()
        #print(query)

    def spotifyorcget(self):
        # gets permalinks from orchestration table where spotify id is missing
        # further used to search for spotify ids
        try:
            self.config.read('/home/ubuntu/airflow/dags/dwl-team3/my_config.ini')
            self.config.sections()
            connection = psycopg2.connect(user=self.config['lake']['user'],
                                          password=self.config['lake']['password'],
                                          host=self.config['lake']['host'],
                                          port=self.config['lake']['port'],
                                          database=self.config['lake']['database'])

            cursor = connection.cursor()

            postgres_lookup = """SELECT permalink, spotify_track_id FROM lakeOrchestra WHERE spotify_track_id IS NULL"""

            cursor.execute(postgres_lookup)
            result = cursor.fetchall()
            self.linklist = []

            for i in range(len(result)):
                self.linklist.append(result[i][0])
            #print(self.linklist)
        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

    def youtube_orc_get(self):
        # creates initial index for orchestration database
        try:
            self.config.read(self.path)
            self.config.sections()
            connection = psycopg2.connect(user=self.config['lake']['user'],
                                          password=self.config['lake']['password'],
                                          host=self.config['lake']['host'],
                                          port=self.config['lake']['port'],
                                          database=self.config['lake']['database'])

            cursor = connection.cursor()

            postgres_lookup = """SELECT permalink, youtube_video_id FROM lakeOrchestra WHERE youtube_video_id IS NULL"""

            cursor.execute(postgres_lookup)
            result = cursor.fetchall()
            self.linklist = []

            for i in range(len(result)):
                self.linklist.append(result[i][0])
        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)



#test = conductor()
#test.youtubeorcget()

#test = conductor()
#test.redditorcget()
#test.redditorcwrite()
#test.updateredditorc()
#test.spotifyorcget()



#%%
