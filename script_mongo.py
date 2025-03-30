import os
import json
import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus

usuario = "jspereira0402"
contraseña = "admin"
uri = f"mongodb+srv://{usuario}:{quote_plus(contraseña)}@cluster0.wx6lt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri)
db = client["lab07"]

for filename in os.listdir("."):
    if filename.endswith(".json"):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)

                if isinstance(data, dict):
                    data = [ {"clave": k, **v} if isinstance(v, dict) else {"clave": k, "valor": v} for k, v in data.items() ]

                df = pd.DataFrame(data)

                if "_id" in df.columns:
                    df = df.drop(columns=["_id"])
                df = df.drop_duplicates()
                df = df.fillna("N/A")
                for col in df.select_dtypes(include="object").columns:
                    df[col] = df[col].astype(str).str.strip().str.title()

                collection_name = os.path.splitext(filename)[0]
                collection = db[collection_name]

                collection.delete_many({})
                collection.insert_many(df.to_dict(orient="records"))

                print(f"'{filename}' insertado como colección '{collection_name}'.")

        except Exception as e:
            print(f"Error al procesar '{filename}': {e}")
