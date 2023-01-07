import json
import requests
import praw
import pandas as pd
import os
import psycopg2
#from sqlalchemy import create_engine
#from sqlalchemy import
from datetime import datetime
from datetime import timezone
from dwlrds import *
from ormtables import *
import configparser

# File to fetch data from reddit threads
# Path: dwl-team3/RedditAPIClass.py
# All interactions with the Reddit API are done here
# subreddit list supplied by main function, can be modified at will if we find more subs



#subreddit_list = ["worldmusic", "popheads", "indieheads", "listentothis", "newmusic", "hiphopheads", "promoteyourmusic", "music", "letstalkmusic", "listige", "independentmusic", "musicinthemaking", "radioreddit", "wereonspotify", "mymusic", "musicsuggestions", "thisisourmusic", "theseareouralbums", "shareyourmusic", "acousticoriginals", "composer"]

class redditGetter:

    def __init__(self, subreddit_list, postlimit=1000):
        self.subreddit_list = subreddit_list # list of subreddits to be queried, user input
        self.dbConn = rdsSession('dwl_lake')
        self.postlimit = postlimit
        self.config = configparser.ConfigParser()
        #self.subreddit_list = ["radioreddit", "worldmusic", "popheads", "indieheads", "listentothis", "newmusic", "hiphopheads", "promoteyourmusic", "music", "letstalkmusic", "listige", "independentmusic", "musicinthemaking", "wereonspotify", "mymusic", "musicsuggestions", "thisisourmusic", "theseareouralbums", "shareyourmusic", "acousticoriginals", "composer"]

        pass


    def authenticateReddit(self):
        self.config = configparser.ConfigParser()
        self.config.read('my_config.ini')
        self.config.sections()

        self.reddit = praw.Reddit(
                client_id="n32iN1fS7bP7Fj1Xmu0Wug",
                client_secret="AbwHTs4DtdmTTjnqW8D_LPhv_tYH4Q",
                password="DWLpassword",
                user_agent="MyBot/0.0.1",
                username="DWLproject",
        )
        #print("Reddit authenticated")


    def openDBConnection(self):
        self.dbConn.open_session()
        pass

    def closeDBConnection(self):
        self.dbConn.close_session()
        pass

    def tryextract(self, entry):
        # try to extract the value from the entry, othwerise return '0'
        try:
            return entry
        except:
            return '0'
        finally:
            pass

    def redditWrite(self, sub):
        # reads a single subreddit and its posts and writes to database
        # method may need renaming and refactoring for better readability
        subreddit = self.reddit.subreddit(sub)
        #print('Processing ' + sub)

        # Get current time
        #dt = datetime.datetime.now(timezone.utc)
        #utc_time = dt.replace(tzinfo=timezone.utc)
        #utc_timestamp = utc_time.timestamp()
        #print(utc_timestamp)

        # Get the top XXX posts from the subreddit where XXX is the postlimit specified by user
        # Variable needs refactoring for better readability, should be subreddit_in
        dflistentothistest = [ vars(submission) for submission in subreddit.new(limit=self.postlimit) ]

        for j in range(len(dflistentothistest)):
            try:
                object = redditlinks(
                    subreddit = self.tryextract(str(dflistentothistest[j]['subreddit'])),
                    author_fullname = self.tryextract(str(dflistentothistest[j]['author_fullname'])),
                    title = self.tryextract(str(dflistentothistest[j]['title'])),
                    pwls = self.tryextract(str(dflistentothistest[j]['pwls'])),
                    downs = self.tryextract(str(dflistentothistest[j]['downs'])),
                    top_awarded_type = self.tryextract(str(dflistentothistest[j]['top_awarded_type'])),
                    name = self.tryextract(str(dflistentothistest[j]['name'])),
                    quarantine = self.tryextract(str(dflistentothistest[j]['quarantine'])),
                    upvote_ratio = self.tryextract(str(dflistentothistest[j]['upvote_ratio'])),
                    subreddit_type = self.tryextract(str(dflistentothistest[j]['subreddit_type'])),
                    ups = self.tryextract(str(dflistentothistest[j]['ups'])),
                    total_awards_received = self.tryextract(str(dflistentothistest[j]['total_awards_received'])),
                    category = self.tryextract(str(dflistentothistest[j]['category'])),
                    score = self.tryextract(str(dflistentothistest[j]['score'])),
                    approved_by = self.tryextract(str(dflistentothistest[j]['approved_by'])),
                    author_premium = self.tryextract(str(dflistentothistest[j]['author_premium'])),
                    edited = self.tryextract(str(dflistentothistest[j]['edited'])),
                    gildings = self.tryextract(str(dflistentothistest[j]['gildings'])),
                    content_categories = self.tryextract(str(dflistentothistest[j]['content_categories'])),
                    mod_note = self.tryextract(str(dflistentothistest[j]['mod_note'])),
                    created = self.tryextract(str(dflistentothistest[j]['created'])),
                    wls = self.tryextract(str(dflistentothistest[j]['wls'])),
                    view_count = self.tryextract(str(dflistentothistest[j]['view_count'])),
                    archived = self.tryextract(str(dflistentothistest[j]['archived'])),
                    no_follow = self.tryextract(str(dflistentothistest[j]['no_follow'])),
                    is_crosspostable = self.tryextract(str(dflistentothistest[j]['is_crosspostable'])),
                    pinned = self.tryextract(str(dflistentothistest[j]['pinned'])),
                    over_18 = self.tryextract(str(dflistentothistest[j]['over_18'])),
                    all_awardings = self.tryextract(str(dflistentothistest[j]['all_awardings'])),
                    awarders = self.tryextract(str(dflistentothistest[j]['awarders'])),
                    media_only = self.tryextract(str(dflistentothistest[j]['media_only'])),
                    removed_by = self.tryextract(str(dflistentothistest[j]['removed_by'])),
                    num_reports = self.tryextract(str(dflistentothistest[j]['num_reports'])),
                    distinguished = self.tryextract(str(dflistentothistest[j]['distinguished'])),
                    subreddit_id = self.tryextract(str(dflistentothistest[j]['subreddit_id'])),
                    author_is_blocked = self.tryextract(str(dflistentothistest[j]['author_is_blocked'])),
                    removal_reason = self.tryextract(str(dflistentothistest[j]['removal_reason'])),
                    report_reasons = self.tryextract(str(dflistentothistest[j]['report_reasons'])),
                    discussion_type = self.tryextract(str(dflistentothistest[j]['discussion_type'])),
                    num_comments = self.tryextract(str(dflistentothistest[j]['num_comments'])),
                    permalink = self.tryextract(str(dflistentothistest[j]['permalink'])),
                    url = self.tryextract(str(dflistentothistest[j]['url'])),
                    subreddit_subscribers = self.tryextract(str(dflistentothistest[j]['subreddit_subscribers'])),
                    created_utc = self.tryextract(str(dflistentothistest[j]['created_utc'])),
                    num_crossposts = self.tryextract(str(dflistentothistest[j]['num_crossposts'])),
                    is_video = self.tryextract(str(dflistentothistest[j]['is_video'])),
                )
                # print(object)
                self.dbConn.add_to_table(object)
            except:
                pass
        #print('commencing commit...')
        self.dbConn.commit_session()

        #dflistentothistest = dflistentothis[['subreddit', 'author_fullname', 'title', 'pwls', 'downs', 'top_awarded_type', 'name', 'quarantine', 'upvote_ratio', 'subreddit_type', 'ups', 'total_awards_received', 'category', 'score', 'approved_by', 'author_premium', 'edited', 'gildings', 'content_categories', 'mod_note', 'created', 'wls', 'view_count', 'archived', 'no_follow', 'is_crosspostable', 'pinned', 'over_18', 'all_awardings', 'awarders', 'media_only', 'removed_by', 'num_reports', 'distinguished', 'subreddit_id', 'author_is_blocked', 'removal_reason', 'report_reasons', 'discussion_type', 'num_comments', 'permalink', 'url', 'subreddit_subscribers', 'created_utc', 'num_crossposts', 'is_video']]
        #dflistentothistest = dflistentothistest[dflistentothis.created_utc > utc_timestamp - 3600] # scheduled hourly 1 day in seconds: 86400


    def subRedditLooper(self):
        # Calls redditWrite for each subreddit in the list
        for i in self.subreddit_list:
            self.redditWrite(i)


#testreddit = redditGetter(subreddit_list)

#testreddit.authenticateReddit()
#testreddit.createDB()
#sub = 'letstalkmusic'
#testreddit.subRedditLooper()

def mainRedditGet():
    try:
       subreddit_list = ["radioreddit", "worldmusic", "popheads", "indieheads", "listentothis", "newmusic", "hiphopheads", "promoteyourmusic", "music", "letstalkmusic", "listige", "independentmusic", "musicinthemaking", "wereonspotify", "mymusic", "musicsuggestions", "thisisourmusic", "theseareouralbums", "shareyourmusic", "acousticoriginals", "composer"]


       r = redditGetter(subreddit_list, 50)
       r.authenticateReddit()
       r.openDBConnection()
       r.subRedditLooper()
       r.closeDBConnection()

    except:
        print("Error encountered")

subreddit_list = ["theseareouralbums", "shareyourmusic", "acousticoriginals", "composer"]





#mainRedditGet()
#r = redditGetter(subreddit_list)
#r.authenticateReddit()
#r.openDBConnection()
#r.createDB()
#r.subRedditLooper()
#r.closeDBConnection()

