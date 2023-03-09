import json
import boto3
from sqlalchemy import create_engine
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucketName = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    
        
    response = s3_client.get_object(Bucket=bucketName, Key=s3_file_name)
    data = pd.read_csv(response['Body'])
    data = data.drop('Unnamed: 0', axis=1)
    
    engine = create_engine('mysql+pymysql://root:rootroot@jump-capstone-db.c9eqfr5fscvh.us-west-2.rds.amazonaws.com/jump_capstone_db')
    data.to_sql(con=engine, name='Car_Auctions', index=False, if_exists='append')
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
