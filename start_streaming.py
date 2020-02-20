from __future__ import absolute_import, print_function

from twython import Twython, TwythonError
from kafka import KafkaProducer
from tweepy import OAuthHandler, Stream, StreamListener,API
from pymongo import MongoClient
import json
import time
import re
import twint
import datetime
import unidecode

def retriveStats(tweet_retrived,screen_name,date,tweet_id):

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

    # Run
    twint.run.Search(c)

    for tweet in tweets:
        print(tweet.username,":",tweet.id, "|",tweet_id)
        if (tweet.id == tweet_id):
            tweet_retrived["favorite_count"] = tweet.replies_count
            tweet_retrived["retweet_count"] = tweet.retweets_count
            tweet_retrived["likes_count"] = tweet.likes_count
            break;



filtroFashion = ["fashion", "moda", "sfilata"]
filtroMusica = ["musica", "rock", "rap", "hip pop", "concerto"]
filtroFitness = ["fitness", "palestra","workout","pesi"]
filtroSport = ["sport", "calcio", "basket", "tennis"]

def OutputTweet(category,msg):
    dictOutput = {}
    dictOutput["created_at_Date"] = str(datetime.date.today())
    dictOutput["created_at_Time"] = str(datetime.datetime.now().time())[:8]
    dictOutput["screen_name"] = msg["user"]["screen_name"]
    dictOutput["id_tweet"] = msg["id"]
    dictOutput["id_Author"] = msg["user"]["id"]
    dictOutput["followers_count_author"] = msg["user"]["followers_count"]
    dictOutput["friends_count_author"] = msg["user"]["friends_count"]
    dictOutput["listed_count_author"] = msg["user"]["listed_count"]
    dictOutput["created_at_author"] = msg["user"]["created_at"]
    dictOutput["favourites_count_author"] = msg["user"]["favourites_count"]


    if (msg.get("extended_tweet") != None):
        dictOutput["text"] = msg["extended_tweet"]["full_text"]
    else:
        dictOutput["text"] = unidecode.unidecode(msg["text"])
    dictOutput["favourites_count"] = 0
    dictOutput["retweet_count"] = 0
    dictOutput["likes_count"] = 0
    dictOutput["entities"] = msg["entities"]
    dictOutput["category"] = category

    return json.dumps(dictOutput)


def fromArrayToRegex(categoriaFiltri):
    regex = ""
    for filtro in categoriaFiltri:
        regex += "|" + filtro
    return regex[1:]

class StdOutListener(StreamListener):


    def on_data(self, data):

        msg = json.loads(data)
        messaggioDaEsaminare = msg["text"].lower()
        dictOutput = {}
        if(msg.get("retweeted_status") != None):
            messaggioDaEsaminare = messaggioDaEsaminare + " " + msg["retweeted_status"]["text"].lower()

        if(not msg["text"].startswith('RT')):
            if (re.findall(fromArrayToRegex(filtroSport),messaggioDaEsaminare)):
                print("Sport")
                dictOutput = OutputTweet("Sport",msg)
                print(dictOutput)
                producer.send("Sport", dictOutput)

            if (re.findall(fromArrayToRegex(filtroFashion),messaggioDaEsaminare)):
                print("Fashion")
                dictOutput = OutputTweet("Fashion",msg)
                print(dictOutput)
                producer.send("Fashion", dictOutput)

            if (re.findall(fromArrayToRegex(filtroMusica),messaggioDaEsaminare)):
                print("Musica")

                dictOutput = OutputTweet("Musica",msg)
                print(dictOutput)
                producer.send("Musica", dictOutput)

            if (re.findall(fromArrayToRegex(filtroFitness),messaggioDaEsaminare)):
                print("Fitness")
                dictOutput = OutputTweet("Fitness", msg)
                print(dictOutput)
                producer.send("Fitness", dictOutput)

            # manda i dati al consumer e a Kafka
            # mycol.insert_one(msg)
            # producer.send("test",data)

        return True

    def on_error(self, status):
        print("errore SPORT:",status)
        return True

consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x : x.encode('utf-8'))
client_mongo = MongoClient('mongodb://localhost:27017/')
mydb = client_mongo.mydatabase
mycol = mydb.tweetsPROVA

if __name__ == '__main__':

    listener4 = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)
    stream4 = Stream(auth, listener4)
    stream4.filter(languages=["it"], track=filtroFashion+filtroMusica+filtroFitness+filtroSport)#, is_async=True
