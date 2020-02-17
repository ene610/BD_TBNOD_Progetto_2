from __future__ import absolute_import, print_function


#deve fare solo una batch analisi ! niente streaming

from kafka import KafkaProducer
from tweepy import OAuthHandler, Stream, StreamListener
from pymongo import MongoClient
import json
import copy
import twint
def retriveStats(tweet_retrived,screen_name,date,tweet_id):
    newTweet = None
    tweets = []
    # Configure
    c = twint.Config()
    # screen name
    c.Username = screen_name
    # c.Stats = True
    c.Hide_output = True
    c.Since = date
    c.Store_object = True
    c.Store_object_tweets_list = tweets
    print(screen_name)
    # Run
    twint.run.Search(c)

    for tweet in tweets:
        print(tweet.username,":",tweet.id, "|",tweet_id)
        if (tweet.id == tweet_id):
            newTweet = copy.deepcopy(tweet_retrived)
            newTweet["replies_count"] = tweet.replies_count
            newTweet["retweet_count"] = tweet.retweets_count
            newTweet["likes_count"] = tweet.likes_count
            break;

    return newTweet

client_mongo = MongoClient('mongodb://localhost:27017/')
mydb = client_mongo.mydatabase
mycol = mydb.Sport

a = mycol.find(no_cursor_timeout=True)

for x in a:

    if(x != None and x["screen_name"]!="fcin1908it"):
        modified = retriveStats(x,x["screen_name"],x["created_at_Date"],x["id_tweet"])
        if (modified != None):
            print(modified)
            print(x)
            mycol.replace_one(x,modified)
a.close()