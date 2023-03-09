from sqlalchemy import create_engine
import boto3
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    pull = s3_client.get_object(Bucket='jump-python-capstone-clean', Key='USA_cars_datasets_clean.csv')
    data = pd.read_csv(pull['Body'])
    data = data.drop('Unnamed: 0', axis=1)
    
    engine = create_engine('mysql+pymysql://root:rootroot@jump-capstone-db.c9eqfr5fscvh.us-west-2.rds.amazonaws.com/jump_capstone_db')
    data.to_sql(con=engine, name='Car_Auctions', index=False, if_exists='replace')

    return {
        'statusCode': 200,
    }