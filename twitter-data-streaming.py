import tweepy
import csv
import boto3 

AWSAccessKeyId='AKIAWVZUC74SU2HOSPEF'
AWSSecretKey='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'

region='ap-south-1'


s3 = boto3.resource(
    service_name='s3',
    region_name=region,
    aws_access_key_id=AWSAccessKeyId,
    aws_secret_access_key=AWSSecretKey
)



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
tweets = tweepy.Cursor(api.search_tweets, q = search_term, lang = 'en', tweet_mode = 'extended').items(50)

with open(f"tweets_2021-12-03.csv",'a', encoding='utf-8_sig',newline='') as csvfile: #using utf-8_sig instead of utf-8 for valid encodding of special chars
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['user_name','timestamp','likes','retweets','text','user_verified','followers_count'])
    for tweet in tweets:
        csvwriter.writerow([tweet.user.screen_name,tweet.created_at,tweet.favorite_count,tweet.retweet_count,tweet.full_text, tweet.user.verified, tweet.user.followers_count])



s3.Bucket('bitcoin-tweets').upload_file(Filename='tweets_2021-12-03.csv', Key='tweets_2021-12-03.csv')
