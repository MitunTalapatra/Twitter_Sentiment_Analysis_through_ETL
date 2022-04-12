import requests
import sqlalchemy

webhook_url = ("https://hooks.slack.com/services/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")

# 1) connecting to postgres

engine = sqlalchemy.create_engine('postgresql://postgres:1234@postgresdb:5432/tweets', echo=True)

# this requires us to install sqlalchemy to connect

# 2) querying data from postgres
tweet = 'SELECT * FROM tweets;'

tweet_query = engine.execute(tweet)

print(tweet_query)

# 3) posting the data on slack

for i in tweet_query:
    text = str(f'A tweet on Flatearth: \n {i[0]} \n has a compound sentiment of: {i[1]}') 
    print(text)
    data = {'text': text}
    requests.post(url = webhook_url, json = data)