import boto3

# Configuración
AWS_REGION = "eu-south-2"  # Cambia a tu región de AWS
DATABASE_NAME = "trade_data_imat3a11"  # Nombre de la base de datos
CRAWLER_NAME = "trade_data_crawler"
S3_TARGET_PATH = "s3://h2datacortialbizuclaudia/data/"  # Cambia con tu bucket
IAM_ROLE = "arn:aws:iam::288761731908:role/AWSGlueServiceRole"  # Cambia con tu rol de Glue

aws_access_key_id="ASIAUGO4KLNCM4EEQD4P"
aws_secret_access_key="6QXfKpMt6HOt1ebn9oCrihX96veCUiOA7+Foz1pH"
aws_session_token="IQoJb3JpZ2luX2VjEKn//////////wEaCmV1LXNvdXRoLTIiRzBFAiBrdl8x53sVT8z8UBdEhDuUuIJOY0JzB/0YAEpND875TQIhAPYSg2Z7W2hlyeqihbNcSDz030YHFRLD1fo2cKT6muWmKukCCEsQABoMMjg4NzYxNzMxOTA4Igw5VNc0vpEIVDncBlAqxgKv8d6RSIkSMm3LBl9nnAqnC2ERVzB1y3rfVeHIMFY9tbUr03gIQHJ6VjE1BV0fTFJ/pchaNUHMdWk+qvgFDJGg1BbIcNOWBfpud0mNwdX0vDtvFt5ZDe0fV2OFnTkKh9gxUIJbC9WBsjpw8Kf3BIzUkonGimHvjIXVrW0+5DXvfuDgaPj1SASAjw0bUV5uMuIHFzm+NwS2qIm83/7DFx1YgH4EXXZM6YzOnveDr8ym6edbARybB+rA9qK0of7Em6q7Gz7a1onrKeVk5lsHePOAlDjjZ6Ky7HO53UC1kRBsw6aL/KnCDBxG0I8hHTZFz1oAqN6VL5mj7kNPiZYEbLvSlnvC/+vN+UGzL1ajtI9rVflgTew1GA4hfmj2xPbTKNPnL4ASc6eMJ5tbV+HLe1aI4uD/LTB5Cc1sf1TXsrR7KhuYABmr3jDK1469BjqnAXu6tKCT7w0XS8SoqRxQugjui/V6+/FQYoVXh/HLIWe+Wq7dY/Rj8XBtBEEV79lbl44khtPYI8LYmpHbt3KUDVHVnLEDcdK5B2Vdk8GFvtPSwtBSyFuOLqQi3/qgpnJ83UpGd6mwCotonzFeoezUK3EOIOP6B7UnHwxHqJrfoQBKS+9Z+hLQ6nEBjNpWyI7XI524993eKnsyBFxKEv7s9xd5lYrY9noD"
# Inicializar el cliente de AWS Glue
glue_client = boto3.client('glue', region_name=AWS_REGION,
                           aws_access_key_id = aws_access_key_id,
                           aws_secret_access_key = aws_secret_access_key,
                           aws_session_token = aws_session_token
                           )

# 1. Crear la base de datos en AWS Glue Data Catalog
def create_database():
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': DATABASE_NAME,
                'Description': 'Base de datos para almacenar metadatos de datos históricos en S3'
            }
        )
        print(f"Base de datos '{DATABASE_NAME}' creada.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"La base de datos '{DATABASE_NAME}' ya existe.")

# 2. Crear el AWS Glue Crawler
def create_crawler():
    try:
        glue_client.create_crawler(
            Name=CRAWLER_NAME,
            Role=IAM_ROLE,
            DatabaseName=DATABASE_NAME,
            Targets={'S3Targets': [{'Path': S3_TARGET_PATH}]},
            TablePrefix="trade_data_"
        )
        print(f"Crawler '{CRAWLER_NAME}' creado.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"El crawler '{CRAWLER_NAME}' ya existe.")

# 3. Ejecutar el Crawler
def start_crawler():
    glue_client.start_crawler(Name=CRAWLER_NAME)
    print(f"Crawler '{CRAWLER_NAME}' iniciado.")

# Ejecutar las funciones
create_database()
create_crawler()
start_crawler()

