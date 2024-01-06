import requests
import pandas as pd

response = requests.get("https://api.coincap.io/v2/assets")
response.status_code

if response:
    print("La conexión fue exitosa. Bien ahí!!!")
else:
    print("Ha ocurrido un error. Qué mal")

data = response.json()

del data["timestamp"]

data2 = data["data"]

df = pd.DataFrame(data2)

df.dtypes

df.isnull().sum()

df.isna().sum()

df["maxSupply"].fillna(0.0, inplace=True)

df["vwap24Hr"].fillna(0.0, inplace=True)

df["explorer"].fillna("Sin datos", inplace=True)

df.isnull().sum()

df.isna().sum()

df.to_csv("C:\\Users\\Iván\\Downloads\\criptos.csv", index=False)