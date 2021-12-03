from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

import boto3 
import pandas as pd

AWSAccessKeyId='AKIAWVZUC74SU2HOSPEF'
AWSSecretKey='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'

region='ap-south-1'


s3 = boto3.resource(
    service_name='s3',
    region_name=region,
    aws_access_key_id=AWSAccessKeyId,
    aws_secret_access_key=AWSSecretKey
)




def twitter_data_collection():
    
    global s3
    
    baerer_token = "AAAAAAAAAAAAAAAAAAAAAL9eVAEAAAAAlzrtLYD30GeMrnrPOwseqkG22kw%3DnYVHvJSEJa6evdQyYI3pKj4RHSLTFNpNYErv1wYDaouNnn3xk0"

    api_key = "SmcmmHAkjutFFPftMckmPWWuH"
    api_secret = "i0ciB7f2egCQlPmKn08K7NZMWNAknhuBe74wzPuWcuOCZ13iXc"
    access_token = "1452266272511647751-KPXZJxXgpBUm4FguKaSvfG2vgr18Za"
    access_token_secret = "ew7N8JNa16xhwUhHBQmRcGSHB3KNYg783fL2yc1QfJXyc"
    
    # Creating the authentication object
    auth = tweepy.OAuthHandler(api_key, api_secret)
    # Setting your access token and secret
    auth.set_access_token(access_token, access_token_secret)
    # Creating the API object while passing in auth information
    api = tweepy.API(auth,wait_on_rate_limit = True)



    search_term = '(bitcoin OR btc OR #bitcoin) -giveaway -filter:retweets -filter:replies'
    tweets = tweepy.Cursor(api.search_tweets, q = search_term, lang = 'en',until = '2021-11-30, tweet_mode = 'extended').items(50)

    with open(f"tweets_2021-11-30.csv",'a', encoding='utf-8_sig',newline='') as csvfile: #using utf-8_sig instead of utf-8 for valid encodding of special chars
      csvwriter = csv.writer(csvfile)
      csvwriter.writerow(['user_name','timestamp','likes','retweets','text','user_verified','followers_count'])
      for tweet in tweets:
        csvwriter.writerow([tweet.user.screen_name,tweet.created_at,tweet.favorite_count,tweet.retweet_count,tweet.full_text, tweet.user.verified, tweet.user.followers_count])



    s3.Bucket('bitcoin-tweets').upload_file(Filename='tweets_2021-11-30.csv', Key='tweets_2021-11-30.csv')
      return 'Whatever you return gets printed in the logs'



  
 #################################################
  
WORkFLOW_DAG_ID = 'twitter_data_dag'
WORKFLOW_START_DATE = datetime.combine(
   datetime.today() - timedelta(1),
   datetime.min.time()) # yesterday

WORKFLOW_SCHEDULE_INTERVAL = '*/10 * * * *'

WORKFLOW_DEFAULT_ARGS = {
 'start_date': WORKFLOW_START_DATE,
 'email_on_failure': True,
 'email_on_retry':rue,
 'retries': 5,
 'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(dag_id = WORkFLOW_DAG_ID,
          start_date = WORKFLOW_START_DATE,
          schedule_interval = WORKFLOW_SCHEDULE_INTERVAL,
          default_args = WORKFLOW_DEFAULT_ARGS,
          pushback = False
  )


run_this = PythonOperator(
    task_id='twitter_data_collection',
    python_callable=twitter_data_collection,
  dag = dag
)

run_this
