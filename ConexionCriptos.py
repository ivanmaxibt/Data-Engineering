# Importo las librerías 
import pandas as pd
import pyodbc

# Valores de la configuración de SQL Server
server = 'LAPTOP-AFF2SUSG\SQLEXPRESS'
database = 'Criptomonedas'

# Establezco la cadena de conexión
connection = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};'
cnxn = pyodbc.connect(connection)

# Leer el archivo CSV en un DataFrame de pandas
df = pd.read_csv('C:\\Users\\Iván\\Downloads\\criptos.csv')

# Inserción de datos en la tabla
cursor = cnxn.cursor()
for index, row in df.iterrows():
    cursor.execute('INSERT INTO Criptomonedas (Nombre, Ranking, Símbolo, Suministro, [Máximo Suministro], [Capitalización de Mercado USD], [Volumen USD], [Precio USD], [Cambio Porcentual 24h], [Precio PPV 24h], [Fuente de Datos]) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   row['name'], row['rank'], row['symbol'], row['supply'], row['maxSupply'], row['marketCapUsd'], row['volumeUsd24Hr'], row['priceUsd'], row['changePercent24Hr'], row['vwap24Hr'], row['explorer'])
    cnxn.commit()

# Consulta SQL para obtener los datos de la tabla
sql_query = "SELECT * FROM Criptomonedas"

# Carga de datos desde la base de datos
df = pd.read_sql(sql_query, cnxn)

# Cierre de la conexión
cnxn.close()

# Guardar los datos en un archivo CSV
df.to_csv('C:\\Users\\Iván\\Downloads\\Criptomonedas.csv', index=False)