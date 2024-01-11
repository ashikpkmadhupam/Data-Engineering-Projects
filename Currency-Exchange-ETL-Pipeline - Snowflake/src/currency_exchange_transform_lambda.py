import json
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    
    file_list = s3.list_objects(Bucket = 'currency-exchange-ashik', Prefix = 'raw_data/to_process/')["Contents"]
    key_list = []
    file_content_list = []
    for item in file_list:
        if item['Size'] > 0 :
            key_list.append(item['Key'])
            file_data = s3.get_object(Bucket='currency-exchange-ashik',Key=item['Key'])['Body'].read()
            file_content_list.append(json.loads(file_data))
        
    i=0
    for file in file_content_list:
        df = pd.DataFrame.from_dict(file['data'])
        file_date = key_list[i].split("/")[2].split("_")[3].split(".")[0]
        date = datetime.strptime(file_date, '%Y-%m-%d').date()
        date_list = []
        for i in range(len(df)):
            date_list.append(date)
            
        df['date'] = date_list
        df['date'] = pd.to_datetime(df['date'])
        i=i+1
        
        file_buf = StringIO()
        df.to_csv(file_buf, header=True, index=False)
        file_buf.seek(0)
        file_name = "transformed_data/currency_exchange_processed_"+str(date)+".csv"
        s3.put_object(Bucket="currency-exchange-ashik", Body=file_buf.getvalue(), Key=file_name)
        
    
    for s3_file in key_list:
        
        copy_source = {
            'Bucket': 'currency-exchange-ashik',
            'Key': s3_file
        }
        target_file_name = "raw_data/processed/"+s3_file.split("/")[-1]
        s3.copy(copy_source, 'currency-exchange-ashik', target_file_name)
        s3.delete_object(Bucket="currency-exchange-ashik",Key=s3_file)