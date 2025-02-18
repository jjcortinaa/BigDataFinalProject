import boto3

# Configuración
AWS_REGION = "eu-south-2"  # Cambia a tu región de AWS
DATABASE_NAME = "trade_data_bronce"  # Nombre de la base de datos
CRAWLER_NAME = "trade_data_crawler_bronce"
S3_TARGET_PATH = "s3://h2datacortialbizuclaudia-bronce/data/"  # Cambia con tu bucket
IAM_ROLE = "arn:aws:iam::288761731908:role/AWSGlueServiceRole"  # Cambia con tu rol de Glue

aws_access_key_id = "ASIAUGO4KLNCDTE7WURE"
aws_secret_access_key = "gUbezr33NqaTSHmTdhD3KzC0ZFAHP439yTNOzqGS"
aws_session_token = "IQoJb3JpZ2luX2VjEOH//////////wEaCmV1LXNvdXRoLTIiRzBFAiEAlyydkdYnr7eKu98Q1/fYOlhXlp0EexQcWnZIZ2v+/CoCIB+Hij4Demw8XENOoF0+3tRJdjcEnjMbifxboYgxxdOXKvICCJP//////////wEQABoMMjg4NzYxNzMxOTA4IgwjUDPltjwA9eIBXIEqxgL4ECNZhlzQhErUdbqtYjAlzJhLCChduEaE44gZALjTwFGUyDxW90TfezOKL9XKz5Z/h02L9UP7+sCSff6tiPyJ5S2H7TmEUZbnv7TpaUsjYBXXcOzZBJJzRzHnwNL/mXUd08q2A9dq09DTNuhObk+5ZmjnPAnITbSQPrf06M0xroyBeek9VOocflnwhHbHDTSM9fO3pUyIj7lNfVUeRWCZUOPwMhzfZw1umdXIrGmw/3/3dYBT7iG2A6iXcSDDGrbcB/cs5pFO/4DSHALfd0aYomIpc//Mp2cbfJUaLZf8OjUmYSpu7q4mm8wy3xnCqEPKp9PvXTKV8JablWljTkBNF/iVud0Vne2YSy9M7cSdk4r4FaPkjpNricSP0soHmiV8kHB9DWNyhp4Wj7RXsXhWeUEJXGk5iR8FeWCtp4kHR8ffQ5xlPjCRldO9BjqnAZd1DvPdCfCUBe5R3tmdMjIs5UJ+V0jVey73K2Ut/3cFMdW9PPRonRzOqtMy9BuUjH5FOhfMjhZqw6/l+HDfZ/xzMjKKOsMV/hBCe9Gt/FRZsb6jdWd4ChBrY3pjTTcsRUhqndoV7d/PRoj1Mtoxqd1SSS3qbZ+rkfOPZr0OMArZkPY5BPIkDyCYf7s9em5ZKM8BhLAEsXa2Tq0+Vu/hCMpnuhNCplR5"


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

