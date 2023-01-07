import googleapiclient.discovery
import pandas as pd
from re import search
from datetime import datetime
import psycopg2
from ormtables import *
from dwlrds import *
from lakeorchestra import *
import re
from youtube_title_parse import get_artist_title
import configparser


class YouTubeCrossReference:

    def __init__(self):
        self.mapping = []
        self.df = pd.DataFrame
        self.dbConn = rdsSession('dwl_lake')
        self.config = configparser.ConfigParser()

    # def get_new_videos(self):
    #     self.dbConn.open_session()
    #     self.new_video_list = self.dbConn.lake_query(
    #         """SELECT youtube_video_id FROM lakeorchestra WHERE spotify_track_id IS NULL AND youtube_video_id IS NOT NULL AND crossref_done = False """)

    def getVideoTitles(self):

        self.config.read('my_config.ini')
        self.config.sections()
        connection = psycopg2.connect(user=self.config['lake']['user'],
                                      password=self.config['lake']['password'],
                                      host=self.config['lake']['host'],
                                      port=self.config['lake']['port'],
                                      database=self.config['lake']['database'])
        cursor = connection.cursor()

        postgres_lookup = """SELECT DISTINCT(video_id), video_title, channel_title, permalink FROM lakeorchestra LEFT JOIN public."YouTubeVideos" ON public."YouTubeVideos".video_id = lakeorchestra.youtube_video_id WHERE lakeorchestra.yt_cleaning_status = 'none' AND lakeorchestra.youtube_video_id IS NOT NULL LIMIT 100"""

        self.df = pd.read_sql_query(postgres_lookup, connection)
        # print(self.df)
        return self.df

    def separateArtistTitle(self):
        AllData = []
        video_titles = self.df['video_title'].to_list()
        video_ids = self.df['video_id'].to_list()
        permalinks = self.df['permalink'].to_list()
        channels_titles = self.df['channel_title'].to_list()

        # print(video_titles)
        # print(video_ids)
        # print(permalinks)

        for i in range(len(video_titles)):
            try:
                video = video_titles[i]
                artist, title = get_artist_title(video)

                print(artist)
                data = dict(artist=artist,
                            song_title=title,
                            video_id=video_ids[i],
                            video_title=video,
                            cross_ref_found=False,
                            permalink=permalinks[i])
                AllData.append(data)
                # print('Artist: ' +artist + '  Song Title: ' + title)

            except TypeError:
                # for i in range(len(video_titles)):
                regex = re.compile('([\s \S]*) - ([\s \S]*)')
                track_title = video_titles[i]
                channel_title = channels_titles[i]

                if channel_title in track_title:
                    extracted_artist = channel_title
                elif channel_title.replace('Music', '').strip() in track_title:
                    extracted_artist = channel_title
                elif "- Topic" in channel_title:
                    extracted_artist = channel_title.replace(' - Topic', '')
                elif "Music" in channel_title:
                    extracted_artist = channel_title
                elif " - " in track_title:
                    extracted_artist = regex.match(track_title).groups()[0]
                else:
                    extracted_artist = channel_title


                if "cover" in track_title or "Cover" in track_title:
                    extracted_track = "EXCLUDE - COVER"
                elif " - " in track_title:
                    if regex.match(track_title).groups()[1] not in channel_title:
                        extracted_track = regex.match(track_title).groups()[1]
                    else:
                        extracted_track = regex.match(track_title).groups()[0]
                elif re.search('[\s]*' ,track_title):
                    extracted_track = track_title
                else:
                    extracted_track = "NOT FOUND"

                data = dict(artist=extracted_artist,
                            song_title=extracted_track,
                            video_id=video_ids[i],
                            video_title=video,
                            cross_ref_found=False,
                            permalink=permalinks[i])

                AllData.append(data)

                #print(extracted_artist + " - " + extracted_track)

        self.df = pd.DataFrame(AllData)
        #print(self.df)

    def clean_data(self):

        try:

            ##### Delete all Cover songs as well as Beats which we do not want to search for!
            patternDel = "(Cover)|(Type Beat)"
            index = self.df['song_title'].str.contains(patternDel, case=False) | self.df['artist'].str.contains(patternDel, case=False)

            self.excluded_df = self.df[index]
            print(self.excluded_df)
            self.df = self.df[~index]
            ###
            ##### Get rid of all parts after the delimiters such as brackts or others (|/#

            self.df['song_title'] = self.df['song_title'].str.split(pat='[(|/#{[@]').str[0]
            self.df['artist'] = self.df['artist'].str.split(pat='[(|/#{[@]').str[0]

            ##### Remove all Features

            self.df['artist'] = self.df['artist'].str.split(pat='(feat.)|(ft.)|(Feat.)|(Ft.)|(&)|(x )').str[0]
            self.df['song_title'] = self.df['song_title'].str.split(pat='(feat.)|(ft.)|(Feat.)|(Ft.)|(&)|(x )').str[0]
            #####

            ##### Get rid of all unwanted characters ('-','.', spaces or non alphanumeric letters like kyrill stay)
            pattern = '\W+[-. ]'

            self.df['artist'] = self.df['artist'].str.replace(pat=pattern, repl='', regex=True)
            self.df['song_title'] = self.df['song_title'].str.replace(pat=pattern, repl='', regex=True)

            ##### Get rid of the anoying '\' which sometimes occurs before approstophes

            self.df['artist'] = self.df['artist'].str.replace(pat="\\", repl='')
            self.df['song_title'] = self.df['song_title'].str.replace(pat="\\", repl='')

            print(self.df)
            return self.df

        except:
            pass


    def add_excluded_data_to_db(self):
        try:
            self.dbConn.open_session()
            self.mapping = self.excluded_df.to_dict(orient='records')
            objects = []

            for row in self.excluded_df.itertuples():
                lake_object = lakeOrchestra(permalink = row.permalink,
                                            yt_cleaning_status = '2')
                self.dbConn.update_clean_yt(lake_object)
            print(objects)
            # print(self.mapping)
            # self.dbConn.bulk_insert_to_table(YouTubeCrossRef, self.mapping)
            self.dbConn.commit_session()

        except:
            pass



    def add_data_to_db(self):

        try:
            self.dbConn.open_session()
            self.mapping = self.df.to_dict(orient='records')
            objects = []

            for row in self.df.itertuples():
                object = YouTubeCrossRef(artist=row.artist,
                                         song_title=row.song_title,
                                         video_id=row.video_id,
                                         video_title=row.video_title,
                                         permalink=row.permalink)
                objects.append(object)
                #UPDATE LAKE ORCHESTRA HERE
                lake_object = lakeOrchestra(permalink = row.permalink,
                                            yt_cleaning_status = '1')
                self.dbConn.update_clean_yt(lake_object)

            print(objects)
            self.dbConn.add_all_to_table(objects)
            # print(self.mapping)
            # self.dbConn.bulk_insert_to_table(YouTubeCrossRef, self.mapping)
            self.dbConn.commit_session()
        except:
            pass

    def getCrossRef(self):
        try:
            self.getVideoTitles()
            self.separateArtistTitle()
            self.clean_data()
            self.add_data_to_db()
            self.add_excluded_data_to_db()
        except:
            pass


#c = YouTubeCrossReference()
#c.getCrossRef()

def yt_to_spotify_crossref_dag():
    c = YouTubeCrossReference()
    c.getCrossRef()


