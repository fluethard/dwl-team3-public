import googleapiclient.discovery
import pandas as pd
from re import search
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from ormtables import *
from dwlrds import *
from lakeorchestra import *
import configparser
import re

# Global Variable Timestamp
Timestamp = datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")


class YouTubeData:

    def __init__(self):
        self.orctuple = None
        self.crossref_youtube_video_id = []
        self.links = []
        self.crossref_df = pd.DataFrame
        self.channel_df = pd.DataFrame
        self.channelBatches = None
        self.batch_size = 50
        self.api_service_name = "youtube"
        self.api_version = "v3"
        self.DEVELOPER_KEY = "AIzaSyCGv4-2MG3lhkKogTk_dDjVVOYM6P_Sv1o"
        self.DEVELOPER_KEY_CROSSREF = "AIzaSyAyKhZuc005D9k-43sWZpCpMNu4sKMduSA"
        self.youtube = googleapiclient.discovery.build(
            self.api_service_name, self.api_version, developerKey=self.DEVELOPER_KEY)
        self.youtube_crossref = googleapiclient.discovery.build(
            self.api_service_name, self.api_version, developerKey=self.DEVELOPER_KEY_CROSSREF)
        self.df = pd.DataFrame
        self.videoIDs = []
        self.linkBatches = []
        self.schema = 'public'
        self.table_name = 'YouTubeVideos'
        self.engine = create_engine("postgresql+psycopg2://postgres:LVrZWb1ossUZraaQuUCn@dwl-production.ccnhtolqau4u"
                                    ".eu-central-1.rds.amazonaws.com:5432/dwl_lake")
        self.links_df = None
        self.youtube_links_df = None
        self.youtube_extracted_df = []
        self.config = configparser.ConfigParser()
        self.dbConn = rdsSession('dwl_lake')
        self.orclist = []
        self.orcdict = {}
        self.artist_df = []

        # A lot of YouTube API Keys were needed to do the initial Cross-Referencing, because of the quote
        # of the YouTube API of 10'000 a day and a search request taking up 100 per search.
        # "AIzaSyCGv4-2MG3lhkKogTk_dDjVVOYM6P_Sv1o" OG
        # "AIzaSyDRWZojBjXSBrD7T_BPtB7sdJ7hDSLgjcA" 1
        # "AIzaSyDQwZ_HIQ9O5VSjKklEaETruFvxSRNq7ps" 2
        # "AIzaSyDmi0QpYnhAqdQowsF0_PVOV8n7ESLJlHs" 3
        # "AIzaSyDYoy_B9O2dhMEmRUtXb9F-KpghIZASb_c" 4
        # "AIzaSyBFVMxLr3O_RDMEBsjalzZIvz6pZpKEO0w" 5
        # "AIzaSyAqtxQW_eNHNlYFfCdqx-ov1431gG2pGec" 6
        # "AIzaSyAyKhZuc005D9k-43sWZpCpMNu4sKMduSA" 7 (CrossRef)

    # Get the reddit permalinks and youtube_video_id where the id is still null! Basis to get leads in next function.
    # Further details in lakeorchestra.py module in conductor class.

    # ************************************* YouTube Videos Functions ************************************************

    def get_orchestration(self):
        orc = conductor()
        orc.youtubeorcget()
        self.orclist = orc.linklist

    # Read reddit leads based on orc list taken from above! Returns a dataframe with permalink and url.
    def read_reddit(self):
        try:
            connection = psycopg2.connect(user="postgres",
                                          password="LVrZWb1ossUZraaQuUCn",
                                          host="dwl-production.ccnhtolqau4u.eu-central-1.rds.amazonaws.com",
                                          port="5432",
                                          database="dwl_lake")
            cursor = connection.cursor()

            self.get_orchestration()
            self.orctuple = tuple(self.orclist)

            self.links_df = pd.read_sql_query(
                "SELECT permalink, url FROM redditlinks WHERE permalink IN {}".format(self.orctuple), connection)

            return self.links_df

        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    # Filter on only the YouTube leads form reddit and then extract the video_id with Regex
    def get_video_id(self):
        # Filter out only Spotify Track links, Album links might be processed at a later point
        self.youtube_links_df = self.links_df[self.links_df.url.str.contains('.youtu')]
        self.youtube_extracted_df = self.youtube_links_df.copy()
        self.youtube_extracted_df['videoID1'] = self.youtube_links_df['url'].str.extract('https://youtu.be/(\w{11})')
        self.youtube_extracted_df['videoID2'] = self.youtube_links_df['url'].str.extract(
            'https://www.youtube.com/watch?v=(\w{11})')  # Because of two different patterns of the YouTube links,
        # we need to extract it twice since the str.extract function only handles one pattern!
        self.youtube_extracted_df['videoID'] = self.youtube_extracted_df['videoID1'].where(
            self.youtube_extracted_df['videoID1'].notnull(), self.youtube_extracted_df['videoID2'])
        self.youtube_extracted_df = self.youtube_extracted_df.drop(['videoID1', 'videoID2'], axis=1)
        self.youtube_extracted_df = self.youtube_extracted_df.dropna()
        self.youtube_extracted_df = self.youtube_extracted_df.reset_index(level=None, inplace=False, drop=True)

    # Function to create batches (List or DF in a List of 50 entries each) because we have can extract maximally
    # 50 videos with on request and YouTube gives every user a quota of which a request is 1 quota point and a user
    # has 10'000 points a day. Therefore, 10'000 request which have to be used wisely with a lot of different requests!
    def create_batches(self):
        self.videoIDs = self.youtube_extracted_df
        self.linkBatches = [self.videoIDs[n:n + self.batch_size] for n in range(0, len(self.videoIDs), self.batch_size)]

        return self.linkBatches

    # Function to get the YouTube Video data based on the video_ids which have been extracted from the url and then
    # put into batches with the above functions.
    def get_youtube_videos(self):
        self.dbConn.open_session()
        print('Start Fetching Data')
        for b in range(len(self.linkBatches)):  # Loop through batches of video_ids
            self.links = self.linkBatches[b]['videoID'].to_list()
            permalinks = self.linkBatches[b]['permalink'].to_list()

            # youtube.videos().list is already defined with the developer key etc. in the Class
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics,recordingDetails,status,topicDetails",
                id=self.links,
                videoCategoryId='10')

            response = request.execute()
            # print(response)

            for i in range(len(response['items'])):  # Loop through every video_id from response

                # This step with first putting the items into a variable is needed because of the try:except: statements
                # used since the helper functions do not work as wished inside the object statement!
                kind = response['items'][i]['kind']
                video_etag = response['items'][i]['etag']
                video_id = response['items'][i]['id']
                published_at = response['items'][i]['snippet']['publishedAt']
                channel_id = response['items'][i]['snippet']['channelId']
                channel_title = response['items'][i]['snippet']['channelTitle']
                video_title = response['items'][i]['snippet']['title']
                video_description = response['items'][i]['snippet']['description']
                try:  # Try and Except statements where used for items which are not always available!
                    tags = response['items'][i]['snippet']['tags']
                except KeyError:
                    tags = None
                video_category_id = response['items'][i]['snippet']['categoryId']
                live_broadcast_content = response['items'][i]['snippet']['liveBroadcastContent']
                localized_video_title = response['items'][i]['snippet']['localized']['title']
                localized_video_description = response['items'][i]['snippet']['localized']['description']
                try:
                    default_audio_language = response['items'][i]['snippet']['defaultAudioLanguage']
                except KeyError:
                    default_audio_language = None
                video_duration = response['items'][i]['contentDetails']['duration']
                video_dimension = response['items'][i]['contentDetails']['dimension']
                video_definition = response['items'][i]['contentDetails']['definition']
                video_caption = response['items'][i]['contentDetails']['caption']
                licensed_content = response['items'][i]['contentDetails']['licensedContent']
                content_rating = str(response['items'][i]['contentDetails']['contentRating'])
                upload_status = response['items'][i]['status']['uploadStatus']
                privacy_status = response['items'][i]['status']['privacyStatus']
                licence = response['items'][i]['status']['license']
                embeddable = response['items'][i]['status']['embeddable']
                public_stats_viewable = response['items'][i]['status']['publicStatsViewable']
                made_for_kids = response['items'][i]['status']['madeForKids']
                try:
                    topic_categories = response['items'][i]['topicDetails']['topicCategories']
                except KeyError:
                    topic_categories = None
                try:
                    recording_location = str(response['items'][i]['recordingDetails']['location'])
                except KeyError:
                    recording_location = None
                try:
                    recording_date = response['items'][i]['recordingDetails']['recordingDate']
                except KeyError:
                    recording_date = None
                permalink = permalinks[i]  # Assigning permalink to the object to have the reference to the Reddit post

                # Assigning the items of each video_id to the YouTubeVideos table object from the ORM tables
                # Metadata of this table is imported through the ormtables.py class.
                object = YoutubeVideos(
                    kind=kind,
                    video_etag=video_etag,
                    video_id=video_id,
                    published_at=published_at,
                    channel_id=channel_id,
                    channel_title=channel_title,
                    video_title=video_title,
                    video_description=video_description,
                    tags=tags,
                    video_category_id=video_category_id,
                    live_broadcast_content=live_broadcast_content,
                    localized_video_title=localized_video_title,
                    localized_video_description=localized_video_description,
                    default_audio_language=default_audio_language,
                    video_duration=video_duration,
                    video_dimension=video_dimension,
                    video_definition=video_definition,
                    video_caption=video_caption,
                    licensed_content=licensed_content,
                    content_rating=content_rating,
                    upload_status=upload_status,
                    privacy_status=privacy_status,
                    license=licence,
                    embeddable=embeddable,
                    public_stats_viewable=public_stats_viewable,
                    made_for_kids=made_for_kids,
                    topic_categories=topic_categories,
                    recording_location=recording_location,
                    recording_date=recording_date,
                    timestamp=datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")
                )

                orchestration_object = lakeOrchestra(
                    permalink=permalink,
                    youtube_video_id=video_id
                )

                # Load the object to the YouTubeVideos table as well as the information to the lakeorchestra.
                # These functions used here are based on the ORM functions by SQL Alchemy and implemented in our own
                # class dwlrds.py

                self.dbConn.update_youtube_video_ids(orchestration_object)
                self.dbConn.add_to_table(object)

            self.dbConn.commit_session()  # Commit the opened session at the end, function defined in dwlrds.py.
        print('YouTubeVideos Table Updated')

    # Function to only pull YouTube statistics (views, likes, etc.)
    def get_youtube_video_stats(self):

        self.dbConn.open_session()
        youtube_video_ids = self.dbConn.lake_query("""SELECT video_id FROM "YouTubeVideos" """)

        # Create batches with video ids only
        self.linkBatches = [youtube_video_ids[n:n + self.batch_size] for n in
                            range(0, len(youtube_video_ids), self.batch_size)]

        for b in range(len(self.linkBatches)):
            links = self.linkBatches[b]

            request = self.youtube.videos().list(
                part="statistics",
                id=links)

            response = request.execute()

            for i in range(len(response['items'])):

                video_id = response['items'][i]['id'],
                view_count = response['items'][i]['statistics']['viewCount']
                try:
                    like_count = response['items'][i]['statistics']['likeCount']
                except KeyError:
                    like_count = 0
                favorite_count = response['items'][i]['statistics']['favoriteCount']
                try:
                    comment_count = response['items'][i]['statistics']['commentCount']
                except KeyError:
                    comment_count = 0

                object = YouTubeVideoStatistics(
                    video_id=video_id,
                    view_count=view_count,
                    like_count=like_count,
                    favorite_count=favorite_count,
                    comment_count=comment_count
                )
                self.dbConn.add_to_table(object)  # Add statistics data to the YouTubeVideoStats table
            self.dbConn.commit_session()
        print('YouTube Video Statistics have been fetched and written into the DB!')

    # ************************************* YouTube Channels Functions ***********************************************

    def get_youtube_channels_list(self):
        # Function to get list of channels that have not been processed from orchestration table
        try:
            connection = psycopg2.connect(user="postgres",
                                          password="LVrZWb1ossUZraaQuUCn",
                                          host="dwl-production.ccnhtolqau4u.eu-central-1.rds.amazonaws.com",
                                          port="5432",
                                          database="dwl_lake")
            cursor = connection.cursor()

            # SQL Query to only get unprocessed channel_ids from YouTubeVideos Table.
            postgres_channel_lookup = """SELECT "YouTubeVideos".channel_id, lakeorchestra.permalink FROM lakeorchestra 
            LEFT JOIN "YouTubeVideos" ON lakeorchestra.youtube_video_id = "YouTubeVideos".video_id 
            WHERE lakeorchestra.youtube_video_id IS NOT NULL AND lakeorchestra.youtube_channel_id IS NULL)"""

            self.channel_df = pd.read_sql_query(postgres_channel_lookup, connection)

        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    def get_youtube_channels(self):

        self.dbConn.open_session()

        # Create batches of channel_ids and permalinks to be requested together.
        self.channelBatches = [self.channel_df[n:n + self.batch_size] for n in
                               range(0, len(self.channel_df), self.batch_size)]

        # Looping through all the batches and making the request from the YouTubeAPI.
        # Set up is the same as with the YouTubeVideos and therefore not further explained here!
        for b in range(len(self.channelBatches)):
            channel_ids = self.channelBatches[b]['channel_id'].to_list()
            permalinks = self.channelBatches[b]['permalink'].to_list()
            request = self.youtube.channels().list(
                part="snippet,contentDetails,statistics,brandingSettings,contentOwnerDetails,id,localizations,status,"
                     "topicDetails",
                id=channel_ids)

            response = request.execute()

            for i in range(len(response['items'])):
                kind = response['items'][i]['kind']
                channel_etag = response['items'][i]['etag']
                channel_id = response['items'][i]['id']
                channel_title = response['items'][i]['snippet']['title']
                channel_description = response['items'][i]['snippet']['description']
                try:
                    custom_url = response['items'][i]['snippet']['customUrl']
                except KeyError:
                    custom_url = None
                published_at = response['items'][i]['snippet']['publishedAt']
                thumbnail = response['items'][i]['snippet']['thumbnails']['high']['url']
                try:
                    localized_channel_title = response['items'][i]['snipped']['localized']['title']
                except KeyError:
                    localized_channel_title = None
                try:
                    localized_channel_description = response['items'][i]['snippet']['localized']['description']
                except KeyError:
                    localized_channel_description = None
                try:
                    country = response['items'][i]['snippet']['country']
                except KeyError:
                    country = None
                try:
                    default_language = response['items'][i]['snippet']['defaultLanguage']
                except KeyError:
                    default_language = None
                related_playlist_likes = response['items'][i]['contentDetails']['relatedPlaylists']['likes']
                related_playlists_uploads = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
                try:
                    topic_categories = response['items'][i]['topicDetails']['topicCategories']
                except KeyError:
                    topic_categories = None
                privacy_status = response['items'][i]['status']['privacyStatus']
                is_linked = response['items'][i]['status']['isLinked']
                long_uploads_status = response['items'][i]['status']['longUploadsStatus']
                try:
                    made_for_kids = response['items'][i]['status']['madeForKids']
                except KeyError:
                    made_for_kids = None
                branding_settings_channel_title = response['items'][i]['brandingSettings']['channel']['title']
                try:
                    branding_settings_channel_description = response['items'][i]['brandingSettings']['channel'][
                        'description']
                except KeyError:
                    branding_settings_channel_description = None
                try:
                    branding_settings_channel_keywords = response['items'][i]['brandingSettings']['channel']['keywords']
                except KeyError:
                    branding_settings_channel_keywords = None
                try:
                    branding_settings_channel_default_language = response['items'][i]['brandingSettings']['channel'][
                        'defaultLanguage']
                except KeyError:
                    branding_settings_channel_default_language = None
                try:
                    branding_settings_channel_country = response['items'][i]['brandingSettings']['channel']['country']
                except KeyError:
                    branding_settings_channel_country = None
                try:
                    content_owner_id = response['items'][i]['contentOwnerDetails']['contentOwner']
                except KeyError:
                    content_owner_id = None
                try:
                    content_owner_time_linked = response['items'][i]['contentOwnerDetails']['timeLinked']
                except KeyError:
                    content_owner_time_linked = None
                a = channel_ids.index(channel_id)
                # Needed because the response output is completely random without any order given in the input!
                # Therefore, we need to match it again through this function and then determining the right permalink
                # for the DB.
                permalink = permalinks[a]

                # After assigning all the items from a specific channel and fixing the issue of the randomness of the
                # request and assigning the right permalink to the channel we can assign it to the YouTubeChannels table
                # object.
                object = YouTubeChannels(kind=kind,
                                         channel_etag=channel_etag,
                                         channel_id=channel_id,
                                         published_at=published_at,
                                         channel_title=channel_title,
                                         channel_description=channel_description,
                                         custom_url=custom_url,
                                         thumbnail=thumbnail,
                                         localized_channel_title=localized_channel_title,
                                         localized_channel_description=localized_channel_description,
                                         country=country,
                                         default_language=default_language,
                                         related_playlist_likes=related_playlist_likes,
                                         related_playlists_uploads=related_playlists_uploads,
                                         is_linked=is_linked,
                                         long_uploads_status=long_uploads_status,
                                         branding_settings_channel_title=branding_settings_channel_title,
                                         branding_settings_channel_description=branding_settings_channel_description,
                                         branding_settings_channel_keywords=branding_settings_channel_keywords,
                                         branding_settings_channel_default_language=branding_settings_channel_default_language,
                                         branding_settings_channel_country=branding_settings_channel_country,
                                         content_owner_id=content_owner_id,
                                         content_owner_time_linked=content_owner_time_linked,
                                         privacy_status=privacy_status,
                                         made_for_kids=made_for_kids,
                                         topic_categories=topic_categories,
                                         timestamp=datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S"))

                orchestration_object = lakeOrchestra(
                    permalink=permalink,
                    youtube_channel_id=channel_id,
                    youtube_updated=datetime.now())

                # Load the created ormtables object to the DB and then commit the session!
                self.dbConn.update_youtube_channel_ids(orchestration_object)
                self.dbConn.add_to_table(object)
            self.dbConn.commit_session()
        print('YouTubeChannelData has been Updated!')

    def get_youtube_channel_stats(self):
        # Function to get the channel statistics daily.

        self.dbConn.open_session()
        youtube_channels_ids = self.dbConn.lake_query("""SELECT channel_id FROM "YouTubeChannels" """)

        # Create batches for the channel_ids for each request.
        self.linkBatches = [youtube_channels_ids[n:n + self.batch_size] for n in
                            range(0, len(youtube_channels_ids), self.batch_size)]

        for b in range(len(self.linkBatches)):
            links = self.linkBatches[b]

            request = self.youtube.channels().list(
                part="statistics",
                id=links)

            response = request.execute()

            # Loop through every item and assign it to the YouTubeChannelStatistics table object and load to DB.
            for i in range(len(response['items'])):
                channel_id = response['items'][i]['id'],
                view_count = response['items'][i]['statistics']['viewCount']
                subscriber_count = response['items'][i]['statistics']['subscriberCount']
                hidden_subscriber_count = response['items'][i]['statistics']['hiddenSubscriberCount']
                video_count = response['items'][i]['statistics']['videoCount']

                object = YouTubeChannelStatistics(
                    channel_id=channel_id,
                    view_count=view_count,
                    subscriber_count=subscriber_count,
                    hidden_subscriber_count=hidden_subscriber_count,
                    video_count=video_count
                )
                self.dbConn.add_to_table(object)
            self.dbConn.commit_session()
        print('YouTube Channel Statistics have been fetched and written into the DB!')

    # ************************************* Cross-Reference Functions *******************************************

    def get_cross_ref_video(self):
        # Function to pull the information for the cross-referenced videos from Spotify. This function was needed
        # because the other function to pull video data has a different set-up

        self.dbConn.open_session()
        print('Start Fetching Data')

        # Make a request with the found video_id from the search_youtube_video function. In this part we do not make
        # batches because we anyway have to search each cross-referenced song individually and can pull them directly
        # when we are able to get the connection!
        request = self.youtube.videos().list(
            part="snippet,contentDetails,statistics,recordingDetails,status,topicDetails",
            id=self.crossref_youtube_video_id)

        response = request.execute()
        print(response)

        # This step with first putting the items into a variable is needed because of the try: except: statements
        # used since the helper functions do not work as wished inside the object statement!
        kind = response['items'][0]['kind']
        video_etag = response['items'][0]['etag']
        video_id = response['items'][0]['id']
        published_at = response['items'][0]['snippet']['publishedAt']
        channel_id = response['items'][0]['snippet']['channelId']
        channel_title = response['items'][0]['snippet']['channelTitle']
        video_title = response['items'][0]['snippet']['title']
        video_description = response['items'][0]['snippet']['description']
        try:
            tags = response['items'][0]['snippet']['tags']
        except KeyError:
            tags = None
        video_category_id = response['items'][0]['snippet']['categoryId']
        live_broadcast_content = response['items'][0]['snippet']['liveBroadcastContent']
        localized_video_title = response['items'][0]['snippet']['localized']['title']
        localized_video_description = response['items'][0]['snippet']['localized']['description']
        try:
            default_audio_language = response['items'][0]['snippet']['defaultAudioLanguage']
        except KeyError:
            default_audio_language = None
        video_duration = response['items'][0]['contentDetails']['duration']
        video_dimension = response['items'][0]['contentDetails']['dimension']
        video_definition = response['items'][0]['contentDetails']['definition']
        video_caption = response['items'][0]['contentDetails']['caption']
        licensed_content = response['items'][0]['contentDetails']['licensedContent']
        content_rating = str(response['items'][0]['contentDetails']['contentRating'])
        upload_status = response['items'][0]['status']['uploadStatus']
        privacy_status = response['items'][0]['status']['privacyStatus']
        licence = response['items'][0]['status']['license']
        embeddable = response['items'][0]['status']['embeddable']
        public_stats_viewable = response['items'][0]['status']['publicStatsViewable']
        made_for_kids = response['items'][0]['status']['madeForKids']
        try:
            topic_categories = response['items'][0]['topicDetails']['topicCategories']
        except KeyError:
            topic_categories = None
        try:
            recording_location = str(response['items'][0]['recordingDetails']['location'])
        except KeyError:
            recording_location = None
        try:
            recording_date = response['items'][0]['recordingDetails']['recordingDate']
        except KeyError:
            recording_date = None

        # Assigning the items of each video_id to the YouTubeVideos table object from the ORM tables
        # Metadata of this table is imported through the ormtables.py class.

        object = YoutubeVideos(
            kind=kind,
            video_etag=video_etag,
            video_id=video_id,
            published_at=published_at,
            channel_id=channel_id,
            channel_title=channel_title,
            video_title=video_title,
            video_description=video_description,
            tags=tags,
            video_category_id=video_category_id,
            live_broadcast_content=live_broadcast_content,
            localized_video_title=localized_video_title,
            localized_video_description=localized_video_description,
            default_audio_language=default_audio_language,
            video_duration=video_duration,
            video_dimension=video_dimension,
            video_definition=video_definition,
            video_caption=video_caption,
            licensed_content=licensed_content,
            content_rating=content_rating,
            upload_status=upload_status,
            privacy_status=privacy_status,
            license=licence,
            embeddable=embeddable,
            public_stats_viewable=public_stats_viewable,
            made_for_kids=made_for_kids,
            topic_categories=topic_categories,
            recording_location=recording_location,
            recording_date=recording_date,
            timestamp=datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")
        )
        # Add the objects to the tables our data lake DB.
        self.dbConn.add_to_table(object)
        self.dbConn.commit_session()
        self.dbConn.close_session()
        print('YouTubeVideos Table Updated')

    # Function to get the Track Name and Artist name from tracks not yet cross-references (Based on flag in the
    # lakeorchestra table).
    def get_artist_track_from_spotify(self):

        try:
            connection = psycopg2.connect(user="postgres",
                                          password="LVrZWb1ossUZraaQuUCn",
                                          host="dwl-production.ccnhtolqau4u.eu-central-1.rds.amazonaws.com",
                                          port="5432",
                                          database="dwl_lake")
            cursor = connection.cursor()

            # Query to get the spotify data which is not yet cross-referenced. LIMIT is set to 99, because with one
            # API key we can at max process 99 due to the limit of 10'000 points (99 Request a 100 Points is 9900 and
            # then the YouTubeVideo itself brings us to a quota use of 9999).
            postgres_channel_lookup = """SELECT artist_name, track_name, track_id, permalink, crossref_done  
            FROM spotifytracks LEFT JOIN lakeorchestra ON lakeorchestra.spotify_track_id = spotifytracks.track_id 
            WHERE crossref_done = FALSE LIMIT 99"""

            self.crossref_df = pd.read_sql_query(postgres_channel_lookup, connection)

            print(self.crossref_df)

        except (Exception, psycopg2.Error) as error:
            print("Failed to fetch", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    # Function to make a search in the YouTube API with the track name and artist name.
    # Rules are explained in the function itself.
    def search_youtube_video(self):
        self.get_artist_track_from_spotify()
        # Concat the Artist Name and Track name to a search string which is used in the search request.
        self.crossref_df['search_string'] = self.crossref_df['artist_name'] + ' ' + self.crossref_df['track_name']
        self.dbConn.open_session()

        for row in self.crossref_df.itertuples():
            search_string = row.search_string  # Loop through df with itertuples() and assign the values to a variable
            track_name = row.track_name
            artist_name = row.artist_name
            search_list = [track_name.casefold(), artist_name.casefold()]  # List with artist name and track name both
            # made case-insensitive for the search because it can happen that it is a different case in YouTube.

            try:  # First try statement to test if the request can be made due to quota (Happened a lot in
                # development and is also good to have this safety net in production so nothing gets the
                # cross-reference false flag just because it could not be searched due to the quota.

                request = self.youtube_crossref.search().list(
                    part="snippet",
                    maxResults=1,
                    # Max Search results is set to one and therefore only the most relevant video is taken!
                    q=search_string,
                    type='video')

                response = request.execute()

                # print(response) --> Needed in development to check if it really matches the track on spotify!
                video_title = response['items'][0]['snippet']['title']
                channel_title = response['items'][0]['snippet']['channelTitle']
                check_string = video_title.casefold() + ' ' + channel_title.casefold()

                # First check to see if we can find the track name and artist name both in the video_title of the
                # YouTube video we just searched with the request. If both are found in the search_list we take the
                # crossref and if not we go to the elif in which we try again with video_title and channel_tile.
                if all(x in video_title.casefold() for x in search_list):
                    print(True)
                    self.crossref_youtube_video_id = response['items'][0]['id']['videoId']
                    try:  # Another safety mechanism in case the quota is already reached, in case we cannot
                        # crossref any of the given tracks or if the track already exists in our DB due to multiple
                        # posting across the platforms, and we have the leads both for YouTube and spotify.
                        self.get_cross_ref_video()
                    except Exception as e:
                        print(e)
                        self.dbConn.close_session()
                        continue

                    # Update the lakeorchestra table with the cross-referenced tracks.
                    self.dbConn.open_session()
                    orchestration_object = lakeOrchestra(
                        permalink=row.permalink,
                        youtube_video_id=self.crossref_youtube_video_id,
                        spotify_track_id=row.track_id,
                        crossref_done=True,
                        crossref_success=True,
                        all_tasks_done=True
                    )
                    self.dbConn.update_track_crossref_youtube(orchestration_object)
                    self.dbConn.commit_session()
                    self.dbConn.close_session()

                # Check if we can find the Artist Name and Track name (search_list) in the channel_title and video_title
                # combined. It is often the case, that only the track name is in the video_title and the artist name in
                # the channel title, and therefore we chose this second check to see if we can cross-reference it.
                elif all(x in check_string for x in search_list):
                    # print('Track found in video_title together with channel_title!') # Print statement for Dev.
                    self.crossref_youtube_video_id = response['items'][0]['id']['videoId']
                    try:
                        self.get_cross_ref_video()
                    except Exception as e:
                        print(e)
                        self.dbConn.close_session()
                        continue

                    self.dbConn.open_session()
                    orchestration_object = lakeOrchestra(
                        permalink=row.permalink,
                        youtube_video_id=self.crossref_youtube_video_id,
                        spotify_track_id=row.track_id,
                        crossref_done=True,
                        crossref_success=True,
                        all_tasks_done=True
                    )
                    self.dbConn.update_track_crossref_youtube(orchestration_object)
                    self.dbConn.commit_session()
                    self.dbConn.close_session()

                # If we are not able to find the artist and track name with both checks we assume that we cannot cross-
                # reference it and update the lakeorchestra table and set crossref_success to false.
                else:
                    print(False)
                    orchestration_object = lakeOrchestra(
                        permalink=row.permalink,
                        youtube_video_id=None,
                        spotify_track_id=row.track_id,
                        crossref_done=True,
                        crossref_success=False,
                        all_tasks_done=True
                    )
                    self.dbConn.update_track_crossref_youtube(orchestration_object)
                    self.dbConn.commit_session()
                    self.dbConn.close_session()

            except Exception as e:
                print(e)

        print('Spotify Cross Reference is done!')

    # ***************************************** Summarizing Functions ************************************************

    # Summarizing function for the YouTube Video Part.
    def get_youtube_video_data(self):
        self.read_reddit()
        self.get_video_id()
        self.create_batches()
        self.get_youtube_videos()

    # Summarizing function for the YouTube Channel Part.
    def get_youtube_channel_data(self):
        self.get_youtube_channels_list()
        self.get_youtube_channels()


# ******************************************* DAG Functions ****************************************************

# Function created for the Statistics DAG which initializes the YouTubeData Class and triggers VideoData function
def youtube_videos_DAG():
    y = YouTubeData()
    y.get_youtube_video_data()


# Function created for the Statistics DAG which initializes the YouTubeData Class and triggers the VideoStats function
def youtube_video_statistics_DAG():
    y = YouTubeData()
    y.get_youtube_video_stats()


# Function created for the Statistics DAG which initializes the YouTubeData Class and triggers ChannelData function
def youtube_channels_DAG():
    y = YouTubeData()
    y.get_youtube_channel_data()


# Function created for the Statistics DAG which initializes the YouTubeData Class and triggers the ChannelStats function
def youtube_channel_statistics_DAG():
    y = YouTubeData()
    y.get_youtube_channel_stats()


# Function created for CrossRef DAG which initializes the YouTubeData Class and triggers the function for the crossref
def youtube_cross_ref_DAG():
    y = YouTubeData()
    y.search_youtube_video()
