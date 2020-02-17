from flask import Flask, jsonify, request
from flask import render_template
import json

app = Flask(__name__, template_folder='twitter/templates')

parola_counter = []
author_followers = []
hashtags_count = []
urlRoute = "/Fashion"

@app.route(urlRoute)
def get_dashboard_page():
    return render_template('streaming_Fashion.html')

@app.route(urlRoute + '/update_word_counter', methods=['POST'])
def update_tweet_counters_data():

    global parola_counter

    if not request.form not in request.form:
        return "error", 400
    parola_counter = (json.loads(request.data))["Word_Count"]
    return "success", 201

@app.route(urlRoute + '/update_author_follower', methods=['POST'])
def update_author_follower_data():

    global author_followers

    if not request.form not in request.form:
        return "error", 400
    author_followers = (json.loads(request.data))["AuthorFollowers"]
    return "success", 201

@app.route(urlRoute + '/update_hashtags_count', methods=['POST'])
def update_hashtags_count_data():

    global hashtags_count

    if not request.form not in request.form:
        return "error", 400
    hashtags_count = (json.loads(request.data))["hashtags_Count"]

    return "success", 201

@app.route(urlRoute + '/refresh_word_counter')
def refresh_tweet_counters_data():
    return jsonify(sParola_counter = parola_counter)

@app.route(urlRoute + '/refresh_hashtags_counter')
def refresh_hashtags_counter_data():
    return jsonify(sHashtags_Counter = hashtags_count)

@app.route(urlRoute + '/refresh_author_follower')
def refresh_author_follower_data():

    global author_followers

    print("author", author_followers)
    return jsonify(sAuthor = author_followers)

if __name__ == "__main__":
     app.run(host='localhost', port=5003)