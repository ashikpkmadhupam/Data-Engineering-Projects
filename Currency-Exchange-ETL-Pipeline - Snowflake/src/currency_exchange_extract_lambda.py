import requests
import json
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    
    url = "https://currency-exchange.p.rapidapi.com/exchange"
    headers = {
    	"X-RapidAPI-Key": os.environ["api_key"],
    	"X-RapidAPI-Host": "currency-exchange.p.rapidapi.com"
    }
    
    currency_list = ["SGD","MYR","EUR","USD","AUD","JPY","CNH","HKD","CAD","DKK","GBP","RUB","NZD","MXN","IDR","TWD","THB","VND"]
    price_list = []
    for currency in currency_list:
        querystring = {"from":currency,"to":"INR",}
        response = requests.get(url, headers=headers, params=querystring)
        price = {"from_currency":currency,"to_currency":"INR","rate":response.json()}
        price_list.append(price)
    response_data = {"data":price_list}
    json_response = json.dumps(response_data)
    
    file_name_key = 'raw_data/to_process/currency_exchange_rates_'+str(datetime.now().strftime("%Y-%m-%d"))+'.json'
    
    s3 = boto3.client('s3')
    
    s3.put_object(Bucket='currency-exchange-ashik',Key=file_name_key,Body=json_response)