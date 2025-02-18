import boto3
import pandas as pd
import os
from io import BytesIO
aws_access_key_id = "ASIAUGO4KLNCDTE7WURE"
aws_secret_access_key = "gUbezr33NqaTSHmTdhD3KzC0ZFAHP439yTNOzqGS"
aws_session_token = "IQoJb3JpZ2luX2VjEOH//////////wEaCmV1LXNvdXRoLTIiRzBFAiEAlyydkdYnr7eKu98Q1/fYOlhXlp0EexQcWnZIZ2v+/CoCIB+Hij4Demw8XENOoF0+3tRJdjcEnjMbifxboYgxxdOXKvICCJP//////////wEQABoMMjg4NzYxNzMxOTA4IgwjUDPltjwA9eIBXIEqxgL4ECNZhlzQhErUdbqtYjAlzJhLCChduEaE44gZALjTwFGUyDxW90TfezOKL9XKz5Z/h02L9UP7+sCSff6tiPyJ5S2H7TmEUZbnv7TpaUsjYBXXcOzZBJJzRzHnwNL/mXUd08q2A9dq09DTNuhObk+5ZmjnPAnITbSQPrf06M0xroyBeek9VOocflnwhHbHDTSM9fO3pUyIj7lNfVUeRWCZUOPwMhzfZw1umdXIrGmw/3/3dYBT7iG2A6iXcSDDGrbcB/cs5pFO/4DSHALfd0aYomIpc//Mp2cbfJUaLZf8OjUmYSpu7q4mm8wy3xnCqEPKp9PvXTKV8JablWljTkBNF/iVud0Vne2YSy9M7cSdk4r4FaPkjpNricSP0soHmiV8kHB9DWNyhp4Wj7RXsXhWeUEJXGk5iR8FeWCtp4kHR8ffQ5xlPjCRldO9BjqnAZd1DvPdCfCUBe5R3tmdMjIs5UJ+V0jVey73K2Ut/3cFMdW9PPRonRzOqtMy9BuUjH5FOhfMjhZqw6/l+HDfZ/xzMjKKOsMV/hBCe9Gt/FRZsb6jdWd4ChBrY3pjTTcsRUhqndoV7d/PRoj1Mtoxqd1SSS3qbZ+rkfOPZr0OMArZkPY5BPIkDyCYf7s9em5ZKM8BhLAEsXa2Tq0+Vu/hCMpnuhNCplR5"


# Configurar el cliente de S3
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  aws_session_token=aws_session_token)

source_bucket = "h2datacortialbizuclaudia-bronce"  # Reemplaza con el bucket de origen
destination_bucket = "h4datacortialbizuclaudia-plata"  # Reemplaza con el bucket de destino

# Listar archivos CSV en el bucket de origen
response = s3.list_objects_v2(Bucket=source_bucket)
if 'Contents' in response:
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.csv'):
            print(f"Procesando {key}...")
            
            # Descargar el archivo CSV
            csv_obj = s3.get_object(Bucket=source_bucket, Key=key)
            df = pd.read_csv(csv_obj['Body'])
            
            # Convertir a Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            
            # Crear nueva clave manteniendo la estructura y cambiando la extensión
            new_key = key.replace('.csv', '.parquet')
            
            # Subir el archivo Parquet al nuevo bucket
            s3.put_object(Bucket=destination_bucket, Key=new_key, Body=parquet_buffer.getvalue())
            print(f"Archivo convertido y subido a s3://{destination_bucket}/{new_key}")
else:
    print("No se encontraron archivos CSV en el bucket de origen.")