import requests
import json
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
import time
import pandas as pd
#Set the API
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'

params=  {
        'symbol': 'BTC,ETH,LTC,XRP,BCH,ADA,DOT,UNI,DOGE,USDT,BNB,LINK,THETA,VET,MATIC,XLM,ETC,FIL,ATOM,XMR,ZEC,ZIL',
        'convert': 'USD' }
headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': 'xxxxxxx-xxxxxx-xxxxx-xxx-xx' # Add your keys 
        }



# Create producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', #Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8') #json serializer
    )

# Read streaming event
count = 1
#Make GIT request
while True:

    response = requests.get(url,params=params,headers=headers)
    data = response.json()
    df = pd.DataFrame(data['data'].items(),columns=['Symbol', 'Data'])
    df['price_USD'] = df['Data'].apply(lambda x: x['quote']['USD']['price'])
    df['24h_change%'] = df['Data'].apply(lambda x: x['quote']['USD']['percent_change_24h'])
    df['1h_change%'] = df['Data'].apply(lambda x: x['quote']['USD']['percent_change_1h'])
    df['last_updated'] = df['Data'].apply(lambda x: x['quote']['USD']['last_updated'])
   # df['last_updated'] = pd.to_datetime(df['last_updated'])
    df.drop('Data',axis=1,inplace=True)
    #df.set_index('Symbol',inplace= True)
    ccount = 1
    for i in range (len(df.iloc[:,0])):

        producer.send('crypto_test_streaming_topic', df.iloc[i].to_dict())
        print(f"Sending each row to kafka, #{ccount}/22")
        ccount +=1


    print(df)



    print(f'sending data to kafka, #{count}')
    count +=1
    time.sleep(60)
 
