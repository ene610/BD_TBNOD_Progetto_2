import json
import pymongo
import pyspark
import pymongo_spark
from flask import Flask, render_template
from pymongo import MongoClient

data = None


#client_mongo = MongoClient('mongodb://localhost:27017/')
#mydb = client_mongo.mydatabase
#mycol = mydb.provone

#mydoc = mycol.find()

#for x in mydoc: print(x["text"])


from pyspark.sql import SparkSession
from pyspark import SparkContext , SparkConf
from pyspark.sql import SparkSession

from pyspark import SparkContext, SparkConf, SQLContext

# mongodb://username:password@ip:port

uri = "mongodb://127.0.0.1:27017"
# database name
database = "mydatabase"
# collection name
collection = "Sport"

from pyspark.sql import SparkSession
spark = SparkSession\
    .builder\
    .master('local')\
    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/'+database+"."+collection)\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')\
    .getOrCreate()
    #.config('spark.mongodb.output.uri', 'mongodb://user:password@ip.x.x.x:27017/database01.data.coll')\

df01 = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    .option("database",database)\
    .option("collection", collection)\
    .load()
def RowUserFollowerEngagement(row):

    count_likes = 0
    count_listed = 0
    count_retweet = 0

    if(row[3] != None):
        count_likes = int(row[3])
    if (row[4] != None):
        count_listed = int(row[4])
    if (row[5] != None):
        count_retweet = int(row[5])
    engagement_rate_single_tweet = ((count_likes + count_listed +count_retweet)/row[2])#*100

    return (row[0],(row[2],engagement_rate_single_tweet,1))

def countStatusTweet(x,y):
    engage_rate_user = x[1]+y[1]
    counter = x[2] + y[2]
    return (x[0],engage_rate_user,counter)

def ordinaTupla(x):
    return (x[0],x[1][0],x[1][1]*100/x[1][2])

#df01.show()
rdd = df01.rdd.map(lambda x : x[1:]).filter(lambda x : int(x[7])>750).map(lambda x : (x[15],x[14],x[7],x[11],x[12],x[13]))
rdd = rdd.map(RowUserFollowerEngagement).reduceByKey(countStatusTweet).map(ordinaTupla).filter(lambda x: x[2]>10)

fin = rdd.collect()
data = fin
for output in fin:
    print(output)

app = Flask(__name__, template_folder='twitter/templates')
@app.route("/")

def index():
    string_Json = "["
    for dato in data :
        string_Json += "{"
        string_Json += "\"utente\" : " +"\""+ str(dato[0])+"\","
        string_Json += "\"followers\" : " + "" + str(dato[1]) + ","
        string_Json +="\"engagement_rate\" : " + "" + str(dato[2]) + ""
        string_Json += "},"
    string_Json = string_Json[:-1] + "]"
    return render_template("PymongoSport.html", data1=string_Json)

app.run(debug=True,host='localhost', port=7003)

