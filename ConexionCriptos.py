# Importo las librerías
import os
from dotenv import load_dotenv
import psycopg2

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

# Crear tabla staging
cur = conn.cursor()
cur.execute("CREATE TABLE tabla_staging (Nombre VARCHAR(255), Ranking INT, Simbolo VARCHAR(255), Suministro FLOAT, MaximoSuministro FLOAT, CapitalizaciondeMercadoUSD FLOAT, VOLUMENUSD FLOAT, PRECIOUSD FLOAT, CambioPorcentual24H FLOAT, PrecioPPV24H FLOAT, FuentedeDatos VARCHAR(255));")
conn.commit()