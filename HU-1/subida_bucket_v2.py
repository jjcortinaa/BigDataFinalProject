from TradingviewData import TradingViewData, Interval
import os
import pandas as pd
import boto3

aws_access_key_id = "ASIAUGO4KLNCDTE7WURE"
aws_secret_access_key = "gUbezr33NqaTSHmTdhD3KzC0ZFAHP439yTNOzqGS"
aws_session_token = "IQoJb3JpZ2luX2VjEOH//////////wEaCmV1LXNvdXRoLTIiRzBFAiEAlyydkdYnr7eKu98Q1/fYOlhXlp0EexQcWnZIZ2v+/CoCIB+Hij4Demw8XENOoF0+3tRJdjcEnjMbifxboYgxxdOXKvICCJP//////////wEQABoMMjg4NzYxNzMxOTA4IgwjUDPltjwA9eIBXIEqxgL4ECNZhlzQhErUdbqtYjAlzJhLCChduEaE44gZALjTwFGUyDxW90TfezOKL9XKz5Z/h02L9UP7+sCSff6tiPyJ5S2H7TmEUZbnv7TpaUsjYBXXcOzZBJJzRzHnwNL/mXUd08q2A9dq09DTNuhObk+5ZmjnPAnITbSQPrf06M0xroyBeek9VOocflnwhHbHDTSM9fO3pUyIj7lNfVUeRWCZUOPwMhzfZw1umdXIrGmw/3/3dYBT7iG2A6iXcSDDGrbcB/cs5pFO/4DSHALfd0aYomIpc//Mp2cbfJUaLZf8OjUmYSpu7q4mm8wy3xnCqEPKp9PvXTKV8JablWljTkBNF/iVud0Vne2YSy9M7cSdk4r4FaPkjpNricSP0soHmiV8kHB9DWNyhp4Wj7RXsXhWeUEJXGk5iR8FeWCtp4kHR8ffQ5xlPjCRldO9BjqnAZd1DvPdCfCUBe5R3tmdMjIs5UJ+V0jVey73K2Ut/3cFMdW9PPRonRzOqtMy9BuUjH5FOhfMjhZqw6/l+HDfZ/xzMjKKOsMV/hBCe9Gt/FRZsb6jdWd4ChBrY3pjTTcsRUhqndoV7d/PRoj1Mtoxqd1SSS3qbZ+rkfOPZr0OMArZkPY5BPIkDyCYf7s9em5ZKM8BhLAEsXa2Tq0+Vu/hCMpnuhNCplR5"


# Configura el cliente de S3
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  aws_session_token=aws_session_token)

bucket_name = "h2datacortialbizuclaudia-bronce"  # Reemplaza con el nombre de tu bucket
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
            
            data_clone.to_csv(local_file_path, index=False, float_format='%.10f')
            
            s3_key = f'data/{crypto}/{year}/{month}/{crypto}{year}{month}.csv'
            s3.upload_file(local_file_path, bucket_name, s3_key)
            print(f"Archivo {local_file_path} subido a s3://{bucket_name}/{s3_key}")