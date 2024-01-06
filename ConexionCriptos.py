# Importo las librerías
import os
from dotenv import load_dotenv
import psycopg2
import boto3

# Cargo las variables de entorno desde el archivo .env
load_dotenv("C:\\Users\\Iván\\Downloads\\Entregable\\rs.env")

# Conexión a Redshift
conn = psycopg2.connect(
    host=os.getenv("Host"),
    port=os.getenv("port"),
    dbname=os.getenv("Database"),
    user=os.getenv("Usuario"),
    password=os.getenv("Contraseña"),
)

# Carga el archivo CSV en el bucket de S3
s3 = boto3.resource('s3')
bucket = s3.Bucket('criptos')
bucket.upload_file('C:\\Users\\Iván\\Downloads\\criptos.csv', 'criptos.csv')

# Crear tabla staging
with conn.cursor() as cur:
    cur.execute('CREATE TABLE tabla_staging (Nombre VARCHAR(255), Ranking INT, Simbolo VARCHAR(255), Suministro FLOAT, MS FLOAT, CDMUSD FLOAT, VOLUMENUSD FLOAT, PRECIOUSD FLOAT, CP24H FLOAT, PPPV24H FLOAT, FDD VARCHAR(255));')

# Cargar datos en la tabla staging
with conn.cursor() as cur:
    cur.execute("COPY tabla_staging FROM 's3://criptos/criptos.csv' DELIMITER ',' CSV HEADER;")

# Realizar merge con la tabla destino
with conn.cursor() as cur:
    cur.execute('''
        MERGE INTO tabla_destino AS t
        USING tabla_staging AS s
        ON t.columna_clave = s.columna_clave
        WHEN MATCHED THEN
            UPDATE SET t.columna1 = s.columna1, t.columna2 = s.columna2, t.columna1 = s.columna1, t.columna2 = s.columna2, t.columna3 = s.columna3, t.columna4 = s.columna4, t.columna5 = s.columna5, t.columna6 = s.columna6, t.columna7 = s.columna7, t.columna8 = s.columna8, t.columna9 = s.columna9, t.columna10 = s.columna10, t.columna11 = s.columna11
        WHEN NOT MATCHED THEN
            INSERT (columna_clave, columna1, columna2, columna3, columna4, columna5, columna6, columna7, columna8, columna9, columna10, columna11)
            VALUES (s.columna_clave, s.columna1, s.columna2, s.columna3, s.columna4, s.columna5, s.columna6, s.columna7, s.columna8, s.columna9, s.columna10, s.columna11));
    ''')

# Eliminar tabla staging
with conn.cursor() as cur:
    cur.execute('DROP TABLE tabla_staging;')