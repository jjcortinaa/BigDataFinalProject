import boto3


aws_access_key_id = "ASIAUGO4KLNCJ673Q6M5"
aws_secret_access_key = "zjQ7iyVEAZ4A6vVHsWeXkWIzoG+XCCT9fD8nZMYu"
aws_session_token = "IQoJb3JpZ2luX2VjEFIaCmV1LXNvdXRoLTIiRjBEAiBSw3YQdySqYmRdQeho62d/QhmeDs+6v0Tl8ZGSE0CopAIgdoZRh84DzDZz3HcYiPt3V83M+ev27WMkpEmeFz4Q2goq8gII9P//////////ARAAGgwyODg3NjE3MzE5MDgiDDXxUKqf90X6Do92ZSrGAgDJVItxJpnHsA1RwUfV7nQv6tbgNtx+K//u3jngxzX3fROrzKVMoQmPqQ/KjLy8QuKXBj+hUcWpJFh4JNeH+V7EkD5IpwB9JHNu4YGfY3/fdMTOL/V5GYrWyNn/IJhNbS9oVaMjq9aJcrvvBXzd1sNf4imXcwGlawGuF4lPGIZTR4qVhnwfh+8ZVBS0aTKRoHnbV+5ROMfqhAw9aPlDhSI3RuDl9INzKirXBjGzheCuYiIMWzScAA16n25ScbN1LPA4FUhxIsW6uM76D+P0DqAnTqV7+InwGM5zIDJiiFBpjOYncNX3LKS/mmfMLES0sShgoGxlcZr0C6KWuJMFuiztUVCNV90SKArShBJXAX+qwy9kNji8iPep29AHAPsDdbVdPYWTteuMrwfgDGmi3SH3UpmWtjqOn2slu77YCiFlWJAU4WDJMJvRs70GOqgBlx4RHHK1vhHNVE6NMKMOShyK+2E3vmH/hOZiA6ZDqHv40MpmjG3O105bj2PynYP80w/l9gSBakell42edWWI5kSfd1KWtl4VfMck5HWgsGrkxb2ThrKpEXjowvV2KAlmWf0hY1VsOJzvXTGet7eEv6dPvNTijzW1CQ0eAGj3VCHcDWRGcfR5wImfNtMuak575b9S9B/sFi0Cj73NGk3qbMWVnFbnyGvU"
# Configuración
AWS_REGION = "eu-south-2"  # Cambia a tu región de AWS
DATABASE_NAME = "trade_data_imat3a11-2"  # Nombre de la base de datos
CRAWLER_NAME = "trade_data_crawler-2"
S3_TARGET_PATH = "s3://h2datacortialbizuclaudia-2/data/"  # Cambia con tu bucket
IAM_ROLE = "arn:aws:iam::288761731908:role/AWSGlueServiceRole"  # Cambia con tu rol de Glue

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

