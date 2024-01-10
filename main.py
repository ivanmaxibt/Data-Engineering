# Importo las librerías
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

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

# Crear cursor
cur = conn.cursor()

# Leer el archivo CSV en un DataFrame de Pandas
data = pd.read_csv('C:\\Users\\Iván\\Downloads\criptos.csv')

# Insertar los datos en Redshift
data.to_sql('tabla_staging', conn, if_exists='replace', index=False)

# Confirmar cambios
conn.commit()

# Cerrar cursor y conexión
cur.close()
conn.close()