import pymongo
import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sqlalchemy import create_engine
import time
import re

def sentiment_score(text):
    analyzer = SentimentIntensityAnalyzer()
    return analyzer.polarity_scores(text)['compound']


mentions_regex= '@[A-Za-z0-9]+'
url_regex='https?:\/\/\S+' #this will not catch all possible URLs
hashtag_regex= '#'
rt_regex= 'RT\s'

def clean_tweets(tweet):
    tweet = re.sub(mentions_regex, '', tweet)  #removes @mentions
    tweet = re.sub(hashtag_regex, '', tweet) #removes hashtag symbol
    tweet = re.sub(rt_regex, '', tweet) #removes RT to announce retweet
    tweet = re.sub(url_regex, '', tweet) #removes most URLs
    
    return tweet

# Extraction
client = pymongo.MongoClient(host="mongodb", port=27017)
time.sleep(300)
db = client.twitter
our_tweets = db.tweets.find()


# Transform
texts = [clean_tweets(doc['text']) for doc in our_tweets]
scores = [sentiment_score(text) for text in texts]
dictx = {'texts': texts, 'scores': scores}
print(dictx)
print('ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ')

#Load
pg = create_engine('postgresql://postgres:1234@postgresdb:5432/tweets', echo=True)
pg.execute("CREATE TABLE IF NOT EXISTS tweets (text VARCHAR(1000), sentiment NUMERIC);")

query = "INSERT INTO tweets VALUES (%s, %s);"

for text, score in zip(texts, scores):#
	pg.execute(query, (text, score))