import json
from flask import Flask, jsonify, request
from flask import render_template
import pymongo
import pyspark
import pymongo_spark

from pymongo import MongoClient

data = None
uri = "mongodb://127.0.0.1:27017"
database = "mydatabase"

arrayOrarioTweets = []
count_Tweets_Sport = []
count_Tweets_Fashion = []
count_Tweets_Fitness = []
count_Tweets_Musica = []

arrayOrarioHashtags = []
count_Hashtags_Sport = []
count_Hashtags_Fashion = []
count_Hashtags_Fitness = []
count_Hashtags_Musica = []
dataAnalisi = "16/09/2019"
from pyspark.sql import SparkSession

def jobSumTweetCount():

    collection = "allTopicTweet"
    spark = SparkSession \
        .builder \
        .master('local') \
        .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/' + database + "." + collection) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
        .getOrCreate()

    df01 = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("database",database)\
        .option("collection", collection)\
        .load()

    def sumArray(x,y):
        output = [0,0,0,0]
        for i in range(0,4):
            output[i] = x[i] + y[i] + output[i]
        return output

    rdd = df01.rdd.map(lambda x : x[1:]).filter(lambda x : x[1] == dataAnalisi)\
        .map(lambda x : (x[0][:2],x[2])).reduceByKey(sumArray).collect()

    for i in rdd:
        arrayOrarioTweets.append(i[0])
        print(i)
        print(i[1][0])
        count_Tweets_Sport.append(i[1][0])
        count_Tweets_Fashion.append(i[1][1])
        count_Tweets_Fitness.append(i[1][3])
        count_Tweets_Musica.append(i[1][2])

def jobSumHashtagsCount():
    collection = "allTopicHashtags"

    spark = SparkSession \
        .builder \
        .master('local') \
        .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/' + database + "." + collection) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
        .getOrCreate()

    df01 = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("database",database)\
        .option("collection", collection)\
        .load()

    def sumArray(x,y):
        output = [0,0,0,0]
        for i in range(0,4):
            output[i] = x[i] + y[i] + output[i]
        return output

    rdd1 = df01.rdd.map(lambda x : x[1:])#.filter(lambda x : x[1] == dataAnalisi)
    rdd = rdd1.map(lambda x : (x[2][:2],x[1])).reduceByKey(sumArray).sortBy(lambda a : a[0]).collect()

    for line in rdd1.collect():
        print("line",line)

    for i in rdd:
        arrayOrarioHashtags.append(i[0])
        count_Hashtags_Sport.append(i[1][0])
        count_Hashtags_Fashion.append(i[1][1])
        count_Hashtags_Fitness.append(i[1][3])
        count_Hashtags_Musica.append(i[1][2])

jobSumTweetCount()
jobSumHashtagsCount()

app = Flask(__name__, template_folder='twitter/templates')

@app.route("/AllTopicBatch")
def get_dashboard_page():
    global most_used_hashtags, most_active_users, most_mentioned_users
    return render_template('batch_allTopic.html')

@app.route("/AllTopicBatch" + '/refresh_hashtags_counter')
def refresh_hashtags_counters_data():

    return jsonify(
        arrayOrarioHashtags = arrayOrarioHashtags,
        count_Hashtags_Sport = count_Hashtags_Sport,
        count_Hashtags_Fashion = count_Hashtags_Fashion,
        count_Hashtags_Fitness = count_Hashtags_Fitness,
        count_Hashtags_Musica = count_Hashtags_Musica)

@app.route("/AllTopicBatch" + '/refresh_tweets_counter')
def refresh_tweet_counters_data():

    return jsonify(
        arrayOrarioTweets= arrayOrarioTweets,
        count_Tweets_Sport = count_Tweets_Sport,
        count_Tweets_Fashion = count_Tweets_Fashion,
        count_Tweets_Fitness = count_Tweets_Fitness,
        count_Tweets_Musica = count_Tweets_Musica)

if __name__ == "__main__":
     app.run(host='localhost', port=5007)