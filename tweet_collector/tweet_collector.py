import time
from datetime import datetime
import logging
import random
import pymongo
import tweepy as tw


# Create a connection to the MongoDB database server
client = pymongo.MongoClient(host='mongodb', port=27017) # hostname = servicename for docker-compose pipeline

# Create/use a database
db = client.twitter
# equivalent of CREATE DATABASE twitter;

# Define the collection
collection = db.tweets
# equivalent of CREATE TABLE tweets;


#credentials
consumer_key= 'XXXX'
consumer_secret= 'XXXX'
access_token= 'XXXXX'
access_token_secret= 'XXXX'
bearer_token= 'XXXX'

# authentication
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)



# The search term you want to find

# Language code (follows ISO 639-1 standards)
search = "child+abuse -filter:retweets"

# Calling the user_timeline function with our parameters

tweets = tw.Cursor(api.search_tweets,
                   result_type='recent',
                   q=search,
                   lang="en").items(100)

for tweet in tweets:
    tweet = {'text': tweet.text}

    # Insert the tweet into the collection
    logging.warning('-----Tweet being written into MongoDB-----')
    logging.warning(tweet)
    collection.insert_one(tweet) #equivalent of INSERT INTO tweet_data VALUES (....);
    logging.warning(str(datetime.now()))
    logging.warning('----------\n')

    time.sleep(3)