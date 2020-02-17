import json
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
import requests
import datetime
from datetime import date
import numpy as np
#saveToMongo
import pymongo_spark
from pymongo import MongoClient

client_mongo = MongoClient('mongodb://localhost:27017/')
mydb = client_mongo.mydatabase
mycol = mydb.allTopicHashtags


url = 'http://localhost:5005/AllTopic/'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1 pyspark-shell'

sc = SparkContext(appName = "allTopic")
ssc = StreamingContext(sc,60) # (sparkContext, batchDuration
kafkaStream = KafkaUtils.createDirectStream(ssc, ['Sport',"Fashion","Fitness","Musica"], {'metadata.broker.list': 'localhost:9092'})

rdd = kafkaStream.window(60,60)

def TweetsCounterTopic():

    def saveTweetOnMongoDB(line):
        client_mongo = MongoClient('mongodb://localhost:27017/')
        mydb = client_mongo.mydatabase
        mycol = mydb.allTopicTweet
        #print("Tweet",line[1][0],line[1][1],date.today().strftime("%d/%m/%Y"))
        mycol.insert_one({"tweet_count": line[1][0],"date": line[1][1],"time": date.today().strftime("%d/%m/%Y")})
        client_mongo.close()
        return line

    def sendWithRequests(record):
        requests.post(url + "update_category_counter",data = json.dumps({"data" : record[0],"orario": record[1]}))

    def sendDataHtml(x):
        x.foreach(sendWithRequests)

    def rowToArray(record):

        counter_sport = 0
        counter_fashion = 0
        counter_fitness = 0
        counter_musica = 0

        if (record[0] == "Sport"):
            counter_sport = record[1][0]
        if (record[0] == "Fashion"):
            counter_fashion = record[1][0]
        if (record[0] == "Musica"):
            counter_musica = record[1][0]
        if (record[0] == "Fitness"):
            counter_fitness = record[1][0]
        return ("",([counter_sport,counter_fashion,counter_musica,counter_fitness],record[1][2]))

    def sumArray(x,y):
        result = [0,0,0,0]
        for index in range(0,4):
            result[index] =result[index] + x[0][index] + y[0][index]
        return(result,x[1])

    tweet_counter = rdd.mapValues(lambda x: json.loads(x)).map(lambda x : (x[1]["category"],1)).reduceByKey(lambda x,y : x+y)
    tweet_counter = tweet_counter.mapValues(lambda x : (x,date.today().strftime("%d/%m/%Y"),datetime.datetime.now().time().strftime("%H:%M")))

    tweet_counter = tweet_counter.map(rowToArray).map(saveTweetOnMongoDB)
    tweet_counter = tweet_counter.reduceByKey(sumArray).map(lambda x : x[1])
    tweet_counter.foreachRDD(sendDataHtml)

def HashtagsCounterTopic():
    def rowToArray(record):

        counter_sport = 0
        counter_fashion = 0
        counter_fitness = 0
        counter_musica = 0

        if (record[0] == "Sport"):
            counter_sport = record[1]
        if (record[0] == "Fashion"):
            counter_fashion = record[1]
        if (record[0] == "Musica"):
            counter_musica = record[1]
        if (record[0] == "Fitness"):
            counter_fitness = record[1]
        return ("",[counter_sport, counter_fashion, counter_musica, counter_fitness])

    def sumArray(x,y):
        result = [0,0,0,0]

        for index in range(0,4):
              result[index] = result[index] + x[index] + y[index]

        return(result)

    def retriveHashtags(list):
        i = 0
        for hashtag in list["entities"]["hashtags"]:
            i = i +1
        return (list["category"],i)

    def sendWithRequests(record):
        requests.post(url + "update_hashtags_counter_allTopic", data=json.dumps({"data": record[0], "orario": record[1]}))

    def sendDataHtml(x):
        x.foreach(sendWithRequests)

    def saveHashtagsOnMongoDB(line):
        client_mongo = MongoClient('mongodb://localhost:27017/')
        mydb = client_mongo.mydatabase
        mycol = mydb.allTopicHashtags
        mycol.insert_one({"hashtags_count": line[0],"date": line[1],"time": line[2]})
        client_mongo.close()
        return line

    lines = rdd \
        .mapValues(lambda x: json.loads(x)) \
        .mapValues(retriveHashtags)\
        .map(lambda x : x[1])\
        .reduceByKey(lambda x,y : x+y)\
        .map(rowToArray)


    lines = lines.reduceByKey(sumArray)\
        .map(lambda x : (x[1],date.today().strftime("%d/%m/%Y"),datetime.datetime.now().time().strftime("%H:%M")))
    kappa = lines.map(saveHashtagsOnMongoDB)
    kappa.pprint()

    #mycol.insert_one(dataMongo)
    zeta = lines.map(lambda x : (x[0],x[2]))
    zeta.foreachRDD(sendDataHtml)


    #lines.foreachRDD(sendDataHtml)

TweetsCounterTopic()
HashtagsCounterTopic()

def dropMongo(x):
    mycol.drop()
    return x

#sorted.mapValues(dropMongo).map(saveOnMongoDB)

ssc.start()
ssc.awaitTermination()



