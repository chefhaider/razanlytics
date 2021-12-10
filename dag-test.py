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




def test_func():
    
    global s3
    
    foo = pd.DataFrame({'x': [1, 2, 3], 'y': ['a', 'b', 'c']})

    # Save to csv
    foo.to_csv('foo.csv')

    # Upload files to S3 bucket
    s3.Bucket('bitcoin-tweets').upload_file(Filename='foo.csv', Key='test_file_%s-%s-%s_%s:%s' % tuple(datetime.now().timetuple())[:5])
    
    return 'Whatever you return gets printed in the logs'



  
 #################################################
  
WORkFLOW_DAG_ID = 'test-dag'
WORKFLOW_START_DATE = datetime.combine(
   datetime.today() - timedelta(1),
   datetime.min.time()) # yesterday

WORKFLOW_SCHEDULE_INTERVAL = '*/10 * * * *'

WORKFLOW_DEFAULT_ARGS = {
 ‘start_date’: yesterday,
 ‘email_on_failure’: True,
 ‘email_on_retry’: True,
 ‘retries’: 5,
 ‘retry_delay’: datetime.timedelta(minutes=5),
 ‘project_id’: models.Variable.get(‘razanlytics’)
}

dag = DAG(dag_id = WORkFLOW_DAG_ID,
          start_date = WORKFLOW_START_DATE,
          schedule_interval = WORKFLOW_SCHEDULE_INTERVAL,
          default_args = WORKFLOW_DEFAULT_ARGS
  )


run_this = PythonOperator(
    task_id='testing-task',
    python_callable=test_func,
  dag = dag
)

run_this


#datetime.strptime('2021-12-02 00:00:00', '%Y-%m-%d %H:%M:%S')
