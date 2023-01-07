from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Integer, Text, String, Date, DateTime, Float, Boolean, TIMESTAMP, BigInteger
from sqlalchemy import Column, Identity
from datetime import datetime
from datetime import date

# Table definitions for use with SQLAlchemy ORM

Base = declarative_base()


class redditlinks(Base):
    __tablename__ = 'redditlinks'
    id = Column(Integer, Identity(start=1000000, cycle=True))
    subreddit = Column(String)
    author_fullname = Column(String)
    title = Column(String)
    pwls = Column(String)
    downs = Column(String)
    top_awarded_type = Column(String)
    name = Column(String)
    quarantine = Column(String)
    upvote_ratio = Column(String)
    subreddit_type = Column(String)
    ups = Column(String)
    total_awards_received = Column(String)
    category = Column(String)
    score = Column(String)
    approved_by = Column(String)
    author_premium = Column(String)
    edited = Column(String)
    gildings = Column(String)
    content_categories = Column(String)
    mod_note = Column(String)
    created = Column(String)
    wls = Column(String)
    view_count = Column(String)
    archived = Column(String)
    no_follow = Column(String)
    is_crosspostable = Column(String)
    pinned = Column(String)
    over_18 = Column(String)
    all_awardings = Column(String)
    awarders = Column(String)
    media_only = Column(String)
    removed_by = Column(String)
    num_reports = Column(String)
    distinguished = Column(String)
    subreddit_id = Column(String)
    author_is_blocked = Column(String)
    removal_reason = Column(String)
    report_reasons = Column(String)
    discussion_type = Column(String)
    num_comments = Column(String)
    permalink = Column(String, primary_key=True)
    url = Column(String)
    subreddit_subscribers = Column(String)
    created_utc = Column(String)
    num_crossposts = Column(String)
    is_video = Column(String)

    def __repr__(self):
        return "<redditlinks(subreddit='{}', , author_fullname='{}', , title='{}', , pwls='{}', , downs='{}', , top_awarded_type='{}', , name='{}', , quarantine='{}', , upvote_ratio='{}', , subreddit_type='{}', , ups='{}', , total_awards_received='{}', , category='{}', , score='{}', , approved_by='{}', , author_premium='{}', , edited='{}', , gildings='{}', , content_categories='{}', , mod_note='{}', , created='{}', , wls='{}', , view_count='{}', , archived='{}', , no_follow='{}', , is_crosspostable='{}', , pinned='{}', , over_18='{}', , all_awardings='{}', , awarders='{}', , media_only='{}', , removed_by='{}', , num_reports='{}', , distinguished='{}', , subreddit_id='{}', , author_is_blocked='{}', , removal_reason='{}', , report_reasons='{}', , discussion_type='{}', , num_comments='{}', , permalink='{}', , url='{}', , subreddit_subscribers='{}', , created_utc='{}', , num_crossposts='{}', , is_video='{}')>" \
            .format(self.subreddit, self.author_fullname, self.title, self.pwls, self.downs, self.top_awarded_type,
                    self.name, self.quarantine, self.upvote_ratio, self.subreddit_type, self.ups,
                    self.total_awards_received, self.category, self.score, self.approved_by, self.author_premium,
                    self.edited, self.gildings, self.content_categories, self.mod_note, self.created, self.wls,
                    self.view_count, self.archived, self.no_follow, self.is_crosspostable, self.pinned, self.over_18,
                    self.all_awardings, self.awarders, self.media_only, self.removed_by, self.num_reports,
                    self.distinguished, self.subreddit_id, self.author_is_blocked, self.removal_reason,
                    self.report_reasons, self.discussion_type, self.num_comments, self.permalink, self.url,
                    self.subreddit_subscribers, self.created_utc, self.num_crossposts, self.is_video)


class lakeOrchestra(Base):
    __tablename__ = 'lakeorchestra'
    id = Column(Integer, Identity(start=8000000, cycle=True))
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


class spotifyTracks(Base):
    __tablename__ = 'spotifytracks'
    id = Column(Integer, Identity(start=7000000, cycle=True))
    created = Column(TIMESTAMP)
    album_artist = Column(String)
    album_artist_href = Column(String)
    album_artist_id = Column(String)
    album_artist_name = Column(String)
    album_artist_type = Column(String)
    album_artist_uri = Column(String)
    album_available_markets = Column(String)
    album_external_urls = Column(String)
    album_href = Column(String)
    album_id = Column(String)
    album_images = Column(String)
    album_name = Column(String)
    album_release_date = Column(String)
    album_release_date_precision = Column(String)
    album_total_tracks = Column(String)
    album_type = Column(String)
    artist_external_url = Column(String)
    artist_href = Column(String)
    artist_id = Column(String)
    artist_name = Column(String)
    artist_type = Column(String)
    artist_uri = Column(String)
    track_available_markets = Column(String)
    track_disc_number = Column(String)
    track_duration = Column(String)
    track_is_explicit = Column(String)
    track_isrc = Column(String)
    track_external_url = Column(String)
    track_href = Column(String)
    track_id = Column(String, primary_key=True)
    track_is_local = Column(String)
    track_name = Column(String)
    track_popularity = Column(String)
    track_preview_url = Column(String)
    track_number = Column(String)
    track_type = Column(String)
    track_uri = Column(String)


class spotifyArtists(Base):
    __tablename__ = 'spotifyartists'
    id = Column(Integer, Identity(start=6000000, cycle=True))
    created = Column(TIMESTAMP)
    artist_id = Column(String, primary_key=True)
    followers = Column(String)
    genres = Column(String)
    artist_name = Column(String)
    artist_uri = Column(String)
    artist_popularity = Column(String)


class popularityMetrics(Base):
    __tablename__ = 'popularitymetrics'
    id = Column(Integer, Identity(start=10000000, cycle=True), primary_key=True)
    accessed = Column(DateTime, default=datetime.now)
    date = Column(Date, default=date.today)
    object_id = Column(String)
    service = Column(String)
    type = Column(String)
    value_count = Column(Integer)
    followers = Column(Integer)


class YoutubeVideos(Base):
    __tablename__ = 'YouTubeVideos'
    id = Column(Integer, Identity(start=5000000, cycle=True))
    kind = Column(String)
    video_etag = Column(String)
    video_id = Column(String, primary_key=True)
    video_title = Column(String)
    published_at = Column(TIMESTAMP)
    channel_id = Column(String)
    channel_title = Column(String)
    video_description = Column(Text)
    tags = Column(String)
    video_category_id = Column(String)
    live_broadcast_content = Column(String)
    localized_video_title = Column(String)
    localized_video_description = Column(Text)
    default_audio_language = Column(String)
    video_duration = Column(String)
    video_definition = Column(String)
    video_dimension = Column(String)
    video_caption = Column(String)
    licensed_content = Column(Boolean)
    content_rating = Column(String)
    upload_status = Column(String)
    privacy_status = Column(String)
    license = Column(String)
    embeddable = Column(Boolean)
    public_stats_viewable = Column(Boolean)
    made_for_kids = Column(Boolean)
    topic_categories = Column(String)
    recording_location = Column(String)
    recording_date = Column(TIMESTAMP)
    timestamp = Column(String)


class YouTubeVideoStatistics(Base):
    __tablename__ = 'YouTubeVideoStatistics'
    id = Column(Integer, Identity(start=20000000, cycle=True), primary_key=True)
    accessed = Column(DateTime, default=datetime.now)
    date = Column(Date, default=date.today)
    video_id = Column(String)
    view_count = Column(Integer)
    like_count = Column(Integer)
    favorite_count = Column(Integer)
    comment_count = Column(Integer)


class YouTubeChannels(Base):
    __tablename__ = 'YouTubeChannels'
    id = Column(Integer, Identity(start=4000000, cycle=True))
    kind = Column(String)
    channel_etag = Column(String)
    channel_id = Column(String, primary_key=True)
    published_at = Column(DateTime)
    channel_title = Column(String)
    channel_description = Column(Text)
    custom_url = Column(String)
    thumbnail = Column(String)
    localized_channel_title = Column(String)
    localized_channel_description = Column(Text)
    country = Column(String)
    default_language = Column(String)
    related_playlist_likes = Column(String)
    related_playlists_uploads = Column(String)
    is_linked = Column(Boolean)
    long_uploads_status = Column(String)
    branding_settings_channel_title = Column(String)
    branding_settings_channel_description = Column(Text)
    branding_settings_channel_keywords = Column(String)
    branding_settings_channel_default_language = Column(String)
    branding_settings_channel_country = Column(String)
    content_owner_id = Column(String)
    content_owner_time_linked = Column(DateTime)
    privacy_status = Column(String)
    made_for_kids = Column(Boolean)
    topic_categories = Column(String)
    timestamp = Column(DateTime)


class YouTubeChannelStatistics(Base):
    __tablename__ = 'YouTubeChannelStatistics'
    id = Column(Integer, Identity(start=30000000, cycle=True), primary_key=True)
    accessed = Column(DateTime, default=datetime.now)
    date = Column(Date, default=date.today)
    channel_id = Column(String)
    view_count = Column(BigInteger)
    subscriber_count = Column(Integer)
    hidden_subscriber_count = Column(Boolean)
    video_count = Column(Integer)


class YouTubeCrossRef(Base):
    __tablename__ = 'YouTubeCrossRef'
    id = Column(Integer, Identity(start=3000000, cycle=True), primary_key=True)
    artist = Column(String)
    song_title = Column(String)
    video_id = Column(String)
    video_title = Column(String)
    cross_ref_found = Column(Boolean, default=False)
    permalink = Column(String)
