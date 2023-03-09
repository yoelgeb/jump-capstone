import json
import boto3
import pandas as pd

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def clean_hours_left(x):
    if (type(x) is not int):
        if 'days' in x:
            if len(x) == 12:
                return int(x[0:2]) * 24
            else:
                return int(x[0]) * 24
        elif 'hours' in x:
            if len(x) == 13:
                return int(x[0:2])
            else:
                return int(x[0])
        else:
            return x
    else:
        return x

def lambda_handler(event, context):
    bucketName = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    
    print(bucketName)
    print(s3_file_name)
    
    response = s3_client.get_object(Bucket=bucketName, Key=s3_file_name)
    
    data = pd.read_csv(response['Body'])
    
    # Dropping non-relevant columns and duplicates
    clean = data.drop('lot', axis=1)
    clean = clean.drop('Unnamed: 0', axis=1)
    clean = clean.drop_duplicates('vin')

    # Casting price column to float
    clean = clean.astype({"price": float})

    # Capitalizing States and removing extra spaces in state and country
    clean['state'] = clean['state'].apply(lambda x: x.title())
    clean['country'] = clean['country'].apply(lambda x: x.strip())

    # Make all VIN numbers uppercase
    clean['vin'] = clean['vin'].apply(lambda x: x.upper())

    # Dropping country ca
    clean = clean.drop(clean[(clean['country'] == 'canada')].index)
    clean = clean.drop('country', axis=1)
    
    # Renames the condition column to hours_left
    clean = clean.rename(columns={'condition' : 'hours_left'})
    
    # Turns days and hours into just hours and in an int
    clean['hours_left'] = clean['hours_left'].apply(lambda x: clean_hours_left(x))
    
    # Upload dataframe to S3 bucket
    s3_resource.Bucket('jump-python-capstone-clean').put_object(Key='USA_cars_datasets_clean.csv', Body=clean.to_csv())
    
    return {
        'statusCode': 200,
        'body': json.dumps('Cleaning data and uploading to new bucket succss')
    }
    
