from TradingviewData import TradingViewData, Interval
import os
import pandas as pd
import boto3

aws_access_key_id = "ASIAUGO4KLNCJ673Q6M5"
aws_secret_access_key = "zjQ7iyVEAZ4A6vVHsWeXkWIzoG+XCCT9fD8nZMYu"
aws_session_token = "IQoJb3JpZ2luX2VjEFIaCmV1LXNvdXRoLTIiRjBEAiBSw3YQdySqYmRdQeho62d/QhmeDs+6v0Tl8ZGSE0CopAIgdoZRh84DzDZz3HcYiPt3V83M+ev27WMkpEmeFz4Q2goq8gII9P//////////ARAAGgwyODg3NjE3MzE5MDgiDDXxUKqf90X6Do92ZSrGAgDJVItxJpnHsA1RwUfV7nQv6tbgNtx+K//u3jngxzX3fROrzKVMoQmPqQ/KjLy8QuKXBj+hUcWpJFh4JNeH+V7EkD5IpwB9JHNu4YGfY3/fdMTOL/V5GYrWyNn/IJhNbS9oVaMjq9aJcrvvBXzd1sNf4imXcwGlawGuF4lPGIZTR4qVhnwfh+8ZVBS0aTKRoHnbV+5ROMfqhAw9aPlDhSI3RuDl9INzKirXBjGzheCuYiIMWzScAA16n25ScbN1LPA4FUhxIsW6uM76D+P0DqAnTqV7+InwGM5zIDJiiFBpjOYncNX3LKS/mmfMLES0sShgoGxlcZr0C6KWuJMFuiztUVCNV90SKArShBJXAX+qwy9kNji8iPep29AHAPsDdbVdPYWTteuMrwfgDGmi3SH3UpmWtjqOn2slu77YCiFlWJAU4WDJMJvRs70GOqgBlx4RHHK1vhHNVE6NMKMOShyK+2E3vmH/hOZiA6ZDqHv40MpmjG3O105bj2PynYP80w/l9gSBakell42edWWI5kSfd1KWtl4VfMck5HWgsGrkxb2ThrKpEXjowvV2KAlmWf0hY1VsOJzvXTGet7eEv6dPvNTijzW1CQ0eAGj3VCHcDWRGcfR5wImfNtMuak575b9S9B/sFi0Cj73NGk3qbMWVnFbnyGvU"


# Configura el cliente de S3
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  aws_session_token=aws_session_token)

bucket_name = "h2datacortialbizuclaudia-2"  # Reemplaza con el nombre de tu bucket
request = TradingViewData()

list_cryptos = ['BTCUSD', 'ETHUSD', 'XRPUSD', 'SOLUSD', 'DOGEUSD', 'ADAUSD', 'SHIBUSD', 'DOTUSD', 'AAVEUSD', 'XLMUSD']
years = [2021, 2022, 2023, 2024]
months = list(range(1, 13))  # Lista de meses del 1 al 12

for crypto in list_cryptos:
    data = request.get_hist(symbol=crypto, exchange='CRYPTO', interval=Interval.daily, n_bars=1600).reset_index()
    
    # Convertir todas las columnas a double excepto 'datetime' y 'symbol'
    for col in data.columns:
        if col not in ['datetime', 'symbol']:
            data[col] = pd.to_numeric(data[col], errors='coerce')
    
    for year in years:
        for month in months:
            data_clone = data[(data['datetime'].dt.year == year) & (data['datetime'].dt.month == month)]
            
            local_file_path = f'csvs/{crypto}/{year}/{month}/{crypto}{year}{month}.csv'
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            data_clone.to_csv(local_file_path, index=False)
            
            s3_key = f'data/{crypto}/{year}/{month}/{crypto}{year}{month}.csv'
            s3.upload_file(local_file_path, bucket_name, s3_key)
            print(f"Archivo {local_file_path} subido a s3://{bucket_name}/{s3_key}")