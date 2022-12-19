import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
##the api secret keys have been configured in this seperate file. You will not see this for security reasons.
import config
from pathlib import Path
def run_twitter_etl():
    
    ##this part of the code will be used to create the connection between our code and twitter API
    auth = tweepy.OAuthHandler(config.access_key,config.access_secret)
    auth.set_access_token(config.consumer_key,config.consumer_secret)

    #Create the API Object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',#200 is max allowed count
                            
                        count = 200,
                        include_rts = False,
                        tweet_mode = 'extended'     
                            )

    ##Now the data extracted from Twitter is in a complicated JSON format. So we will filter it out using the following piece of code:-

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {
                        'user' : tweet.user.screen_name,
                        'text' : text,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at
        
        }
        tweet_list.append(refined_tweet)


    ##below step is to write this data into a csv using Pandas
    df = pd.DataFrame(tweet_list)
    file_path = Path("C:/Users/adity/Data-engineering/Twitter-data-pipeline-using-AIRFLOW/csv-catcher-folder/adityas_tweets_2.csv")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path)