from flask import Flask, jsonify, request
from flask import render_template
import ast
import json

app = Flask(__name__, template_folder='twitter/templates')

parola_counter = []
author_followers = []
hashtags_count = []
orarioArrivo = ""
counter_Category = []
counter_hashtags = []
orarioArrivo_hashtags = ""
urlRoute = "/AllTopic"

@app.route(urlRoute)
def get_dashboard_page():
    global most_used_hashtags, most_active_users, most_mentioned_users
    return render_template(
        'streaming_AllTopic.html',
        parola_counter=parola_counter
    )

@app.route(urlRoute + '/update_category_counter', methods=['POST'])
def update_tweet_counters_data():

    global counter_Category
    global orarioArrivo

    if not request.form not in request.form:
        return "error", 400
    #parola_counter = (json.loads(request.data))["Word_Count"]
    counter_Category = json.loads(request.data)["data"]
    orarioArrivo = json.loads(request.data)["orario"]

    return "success", 201

@app.route(urlRoute + '/update_hashtags_counter_allTopic', methods=['POST'])
def update_hashtags_counter_allTopic():

    global counter_hashtags
    global orarioArrivo_hashtags

    if not request.form not in request.form:
        return "error", 400
    #parola_counter = (json.loads(request.data))["Word_Count"]
    counter_hashtags = json.loads(request.data)["data"]
    orarioArrivo_hashtags = json.loads(request.data)["orario"]
    print(orarioArrivo_hashtags)

    return "success", 201

@app.route(urlRoute + '/refresh_hashtags_counter')
def refresh_hashtags_counter():
    return jsonify(counter_hashtags = counter_hashtags,orarioArrivo_hashtags = orarioArrivo_hashtags)

@app.route(urlRoute + '/refresh_tweets_counter')
def refresh_tweet_counters_data():
    return jsonify(counter_Category = counter_Category,orarioArrivo = orarioArrivo)

if __name__ == "__main__":
     app.run(host='localhost', port=5005)