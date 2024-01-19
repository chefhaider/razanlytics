from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import tweepy
import boto3
import csv  # Added the missing import for CSV handling
import pandas as pd
import subprocess

from darts.models import TransformerModel
from darts import TimeSeries
from darts.dataprocessing.transformers import Scaler


# AWS credentials
AWSAccessKeyId = ''
AWSSecretKey = ''
region = 'ap-south-1'



# Initialize S3 resource
s3 = boto3.resource(
    service_name='s3',
    region_name=region,
    aws_access_key_id=AWSAccessKeyId,
    aws_secret_access_key=AWSSecretKey
)



def twitter_data_collection():
    """
    Function to collect Twitter data and upload it to S3.
    """

    global s3

    # Twitter API credentials
    bearer_token = ""
    api_key = ""
    api_secret = ""
    access_token = ""
    access_token_secret = ""

    # Creating the authentication object
    auth = tweepy.OAuthHandler(api_key, api_secret)
    # Setting your access token and secret
    auth.set_access_token(access_token, access_token_secret)
    # Creating the API object while passing in auth information
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # Read current time from a file
    with open('~/variable.txt', 'r') as file:
        curr_time = file.read()



    # Create the filename using the current date
    filename = f"data_{curr_time}.csv"

    # Twitter search parameters
    search_term = '(bitcoin OR btc OR #bitcoin) -giveaway -filter:retweets -filter:replies'
    tweets = tweepy.Cursor(api.search_tweets, q=search_term, lang='en', until=curr_time, tweet_mode='extended').items(50)

    # Write tweets to a CSV file
    with open(filename, 'a', encoding='utf-8_sig', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['user_name', 'timestamp', 'likes', 'retweets', 'text', 'user_verified', 'followers_count'])
        for tweet in tweets:
            csvwriter.writerow([tweet.user.screen_name, tweet.created_at, tweet.favorite_count, tweet.retweet_count,
                                tweet.full_text, tweet.user.verified, tweet.user.followers_count])

    # Upload the CSV file to S3
    s3.Bucket('bitcoin-tweets').upload_file(Filename=filename, Key=filename)

    # Update the current time in the file for the next run
    curr_time = [int(val) for val in curr_time]
    date = datetime(curr_time)
    date += timedelta(days=1)
    s = str(date)[:10]

    with open('~/variable.txt', 'w') as file:
        file.write(s)

    return 'Data Successfully collected'


def execute_pyspark_notebook():
    """
    Function to execute a PySpark notebook.
    """
    # Assuming your PySpark notebook is named 'your_notebook.py'
    pyspark_command = 'spark-submit --master local[2] <path to spark>'
    # You can customize the command based on your PySpark notebook setup

    # Execute the PySpark notebook using BashOperator
    BashOperator(
        task_id='execute_pyspark_notebook',
        bash_command=pyspark_command,
        dag=dag
    )



def mlflow_task():
    """
    Function to execute MLflow pipeline.
    """
    # Specify the path to your MLflow pipeline script
    mlflow_script_path = 'mlflow/training_pipeline.py'

    # Execute MLflow pipeline as a subprocess
    subprocess.run(['python', mlflow_script_path])

    return 'MLflow Pipeline executed successfully'


def transformers_inference_task(model_path = 'articats/model/model5.pth.tar', output_path = 'data/output12.csv', input_path = 'data/btc-sent-f4.csv'):
    """
    Function to perform inference using a transformers model.
    """
    df = pd.read_csv(input_path)
    df.time = pd.to_datetime(df.time)
    df = df.set_index('time')
    df = df.sort_index()
    df = df.astype('float')

    df['timestamp'] = df.index.tz_localize(None)

    model = TransformerModel.load(model_path)
    
    series_open = TimeSeries.from_dataframe(df, 'timestamp', 'open')

    open_scaled = Scaler()
    series_open_scaled = open_scaled.fit_transform(series_open)

    #open_train, open_val = series_open_scaled[:-32], series_open_scaled[-32:]
    pred = model.predict(n=12, series=series_open_scaled)
    pred = series_open_scaled[-1].append(pred)


    df_res = pd.DataFrame()
    df_res.index = pred.pd_dataframe().index .append( series_open_scaled[-36:].pd_dataframe().index )

    df_res['previous'] = open_scaled.inverse_transform(series_open_scaled[-36-24:]).pd_series()
    df_res['predicted'] =  open_scaled.inverse_transform(pred).pd_series()

    df_res.to_csv(output_path)
    
    return 





# Define DAG parameters
WORkFLOW_DAG_ID = 'twitter_data_dag'
WORKFLOW_START_DATE = datetime.combine(datetime.today(), datetime.min.time())  # yesterday
WORKFLOW_SCHEDULE_INTERVAL = '30 8 * * *'



WORKFLOW_DEFAULT_ARGS = {
    'start_date': WORKFLOW_START_DATE,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(dag_id=WORkFLOW_DAG_ID,
          start_date=WORKFLOW_START_DATE,
          schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
          default_args=WORKFLOW_DEFAULT_ARGS,
          catchup=False
          )

# Define the task to run the Twitter data collection function
data_collection = PythonOperator(
    task_id='twitter_data_collection',
    python_callable=twitter_data_collection,
    dag=dag
)

# Define the task to execute the PySpark notebook
run_pyspark_notebook = PythonOperator(
    task_id='execute_pyspark_notebook',
    python_callable=execute_pyspark_notebook,
    dag=dag
)


# Define the task to run the MLflow pipeline
mlflow_task = PythonOperator(
    task_id='run_mlflow_pipeline',
    python_callable=mlflow_task,
    dag=dag
)

# Define the task for transformers inference
transformers_inference = PythonOperator(
    task_id='transformers_inference',
    python_callable=transformers_inference_task,
    dag=dag
)


# Set the task dependencies
data_collection >> run_pyspark_notebook  >> mlflow_task >> transformers_inference_task
