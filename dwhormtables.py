from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Integer, Text, String, Date, DateTime, Float, Boolean, TIMESTAMP, BigInteger
from sqlalchemy import Column, Identity
from datetime import datetime
from datetime import date

# Table definitions for use with SQLAlchemy ORM

Base = declarative_base()


class factsheet(Base):
    __tablename__ = 'factsheet'
    id = Column(Integer, Identity(start=1000000, cycle=True), primary_key=True)
    time_id = Column(Integer)
    reddit_permalink = Column(String)
    subreddit_id = Column(String)
    spotify_track_id = Column(String)
    spotify_artist_id = Column(String)
    spotify_track_popularity = Column(Integer)
    spotify_artist_popularity = Column(Integer)
    spotify_artist_follower_count = Column(Integer)
    youtube_video_id = Column(String)
    youtube_channel_id = Column(String)
    youtube_video_views_count = Column(Integer)
    youtube_video_likes_count = Column(Integer)
    youtube_channel_view_count = Column(Integer)
    youtube_channel_subscriber_count = Column(Integer)
    youtube_channel_video_count = Column(Integer)

class time_id(Base):
    __tablename__ = 'time_id'
    time_id = Column(Integer, primary_key=True)
    time = Column(Date)

class spotify_track(Base):
    __tablename__ = 'spotify_track'
    track_id = Column(String, primary_key=True)
    album_artist = Column(String)
    album_artist_href = Column(String)
    album_artist_id = Column(String)
    album_artist_uri = Column(String)
    album_external_urls = Column(String)
    album_id = Column(String)
    album_name = Column(String)
    album_release_date = Column(Date)
    album_total_tracks = Column(Integer)
    album_type = Column(String)
    track_available_markets = Column(String)
    track_disc_number = Column(Integer)
    track_duration = Column(Integer)
    track_is_explicit = Column(Boolean)
    track_isrc = Column(String)
    track_external_url = Column(String)
    track_name = Column(String)
    track_number = Column(Integer)
    track_type = Column(String)
    track_uri = Column(String)

class spotify_artist(Base):
    __tablename__ = 'spotify_artist'
    spotify_artist_id = Column(String, primary_key=True)
    spotify_artist_name = Column(String)
    spotify_artist_genres = Column(String)



class youtube_video(Base):
    __tablename__ = 'youtube_video'
    youtube_video_id = Column(String, primary_key=True)
    video_etag = Column(String)
    video_title = Column(String)
    youtube_video_published_at = Column(Date)
    video_description = Column(String)
    video_definition = Column(String)
    tags = Column(String)
    video_category = Column(String)
    video_duration = Column(String)
    licensed_content = Column(Boolean)
    license = Column(String)
    embeddable = Column(Boolean)
    public_stats_viewable = Column(Boolean)
    made_for_kids = Column(Boolean)
    topic_categories = Column(String)



class youtube_channel(Base):
    __tablename__ = 'youtube_channel'
    youtube_channel_id = Column(String, primary_key=True)
    youtube_channel_title = Column(String)
    youtube_channel_etag = Column(String)
    youtube_channel_published_at = Column(Date)
    youtube_channel_description = Column(String)
    country = Column(String)
    default_language = Column(String)
    related_playlists = Column(String)
    is_linked = Column(Boolean)
    branding_settings_channel_title = Column(String)
    branding_settings_channel_description = Column(String)
    branding_settings_channel_keywords = Column(String)
    branding_settings_channel_language = Column(String)
    branding_settings_channel_country = Column(String)
    privacy_status = Column(String)
    youtube_channel_made_for_kids = Column(Boolean)
    youtube_channel_topic_categories = Column(String)




class reddit_post(Base):
    __tablename__ = 'reddit_post'
    reddit_permalink = Column(String, primary_key=True)
    reddit_post_title = Column(String)
    reddit_post_score = Column(Integer)
    reddit_post_upvote_ratio = Column(Float)
    reddit_content_category = Column(String)
    reddit_create = Column(DateTime)
    reddit_ups = Column(DateTime)
    reddit_subreddit_id = Column(String)
    reddit_post_num_comments = Column(Integer)
    reddit_num_crossposts = Column(Integer)
    reddit_post_url = Column(String)
    reddit_post_is_video = Column(Boolean)
    reddit_post_video_url = Column(String)

class reddit_subreddit(Base):
    __tablename__ = 'reddit_subreddit'
    subreddit_id = Column(String, primary_key=True)
    subreddit_name = Column(String)
    subreddit_subscribers = Column(Integer)

class dwhOrchestra(Base):
    __tablename__ = 'dwhorchestra'
    id = Column(Integer)
    permalink = Column(String, primary_key=True)
    created = Column(DateTime, default=datetime.now)  # testing default functionality
    spotify_track_id = Column(String)
    spotify_artist_id = Column(String)
    spotify_updated = Column(DateTime)
    youtube_video_id = Column(String)
    youtube_channel_id = Column(String)
    youtube_updated = Column(DateTime)
    crossref_done = Column(Boolean, default=False)
    crossref_success = Column(Boolean)
    all_tasks_done = Column(Boolean, default=False)
    yt_cleaning_status = Column(String)


class experimentalOrchestra(Base):
    __tablename__ = 'experimentalorchestra'
    id = Column(Integer)
    permalink = Column(String, primary_key=True)
    created = Column(DateTime, default=datetime.now)  # testing default functionality
    spotify_track_id = Column(String)
    spotify_artist_id = Column(String)
    spotify_updated = Column(DateTime)
    youtube_video_id = Column(String)
    youtube_channel_id = Column(String)
    youtube_updated = Column(DateTime)
    crossref_done = Column(Boolean, default=False)
    crossref_success = Column(Boolean)
    all_tasks_done = Column(Boolean, default=False)
    yt_cleaning_status = Column(String)




#%%
