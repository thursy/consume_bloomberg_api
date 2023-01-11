# import dependencies
import base64
import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, date, timezone

api_key = os.environ.get("api_key")
host = 'bloomberg-market-and-financial-news.p.rapidapi.com'

apple = "aapl:us"
google = "googl:us"
microsoft = "msft:us"

def get_stat(name):
    d = {}
    url = "https://bloomberg-market-and-financial-news.p.rapidapi.com/stock/get-statistics"
    querystring = {"id":name,"template":"STOCK"}
    headers = {
    "X-RapidAPI-Key": api_key,
    "X-RapidAPI-Host": "bloomberg-market-and-financial-news.p.rapidapi.com"}
    
    response = requests.request("GET", url, headers=headers, params=querystring).json()
    
    d['stock'] = name
    for info in response['result'][0]['table']:
        d[info['name']] = info['value']
    return d



#===========================main code===============================#
#==========================entry point=============================#
def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    if pubsub_message == "Invoke":
        apple_stat = get_stat(apple)
        google_stat = get_stat(google)
        microsoft_stat = get_stat(microsoft)

        df = pd.DataFrame.from_dict([apple_stat, google_stat, microsoft_stat])

        columns_name = {}

        for key in df.keys():
            word = key
            replace_char = './)(_ -'
            for m in replace_char:
                word = word.replace(m, '_')
    
            if key[0].isdigit() == True:
                word = "_"+word
    
            columns_name[key] = word

        df.rename(columns=columns_name, inplace=True)
        df['update_at'] = str(datetime.now())
        df['_5Y_Net_Dividend_Growth'] = df.apply(lambda x: x['_5Y_Net_Dividend_Growth'] if pd.isnull(x['_5Y_Net_Dividend_Growth']) else float(x['_5Y_Net_Dividend_Growth'].replace('%', 'e-2')), axis=1)
        df['Dividend_Indicated_Gross_Yield'] = df.apply(lambda x: x['Dividend_Indicated_Gross_Yield'] if pd.isnull(x['Dividend_Indicated_Gross_Yield']) else float(x['Dividend_Indicated_Gross_Yield'].replace('%', 'e-2')), axis=1)
        df['Market_Cap__M_'] = df.apply(lambda x: x['Market_Cap__M_'] if pd.isnull(x['Market_Cap__M_']) else float(x['Market_Cap__M_'].replace(',','')), axis=1)
        df['Shares_Outstanding__M_'] = df.apply(lambda x: x['Shares_Outstanding__M_'] if pd.isnull(x['Shares_Outstanding__M_']) else float(x['Shares_Outstanding__M_'].replace(',','')), axis=1)
        df['Average_Volume__30_day_'] = df.apply(lambda x: x['Average_Volume__30_day_'] if pd.isnull(x['Average_Volume__30_day_']) else float(x['Average_Volume__30_day_'].replace(',','')), axis=1)


        ##Insert to BigQuery
        data_json = df.to_json(orient='records', lines=True).splitlines()
        rows_to_insert = [json.loads(data) for data in data_json]

        print(len(rows_to_insert)," new record(s) are found")

        client = bigquery.Client()
        table_id = 'ds-portofolio.bloomberg_stock_data.stock_stat'
        errors = client.insert_rows_json(table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert))
        if errors == []:
            print("New rows have been added.")
            print(f"{len(rows_to_insert)} record(s) inserted to BigQuery Table")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))       

