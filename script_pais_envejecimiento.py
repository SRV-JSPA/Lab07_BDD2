import pandas as pd
from sqlalchemy import create_engine, types
import mysql.connector

csv_path = "pais_envejecimiento.csv"
df = pd.read_csv(csv_path)

def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "VARCHAR(255)"

table_name = "paisEnvejecimiento"
columns = []

for col in df.columns:
    col_type = infer_sql_type(df[col].dtype)
    columns.append(f"`{col}` {col_type}")

ddl = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  {', '.join(columns)}\n);"

print("DDL generado:\n", ddl)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="16022004",
    database="lab07_bdd2"
)
cursor = conn.cursor()

cursor.execute(ddl)
conn.commit()

engine = create_engine("mysql+mysqlconnector://root:16022004@localhost/lab07_bdd2")
df.to_sql(name=table_name, con=engine, if_exists="append", index=False)

print("¡Tabla creada e importación completa!")

cursor.close()
conn.close()
