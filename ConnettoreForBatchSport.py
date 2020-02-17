from kafka import KafkaConsumer
from pymongo import MongoClient
import json

client_mongo = MongoClient('mongodb://localhost:27017/')
mydb = client_mongo.mydatabase
mycol = mydb.Sport

consumer = KafkaConsumer(
    'Sport',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True

     )

for message in consumer:
    inserimento  = (json.loads(message.value))
    print(inserimento)
    x = mycol.insert_one(inserimento)
    print(x.inserted_id)
