import pandas as pd
import numpy as np
import sqlite3
from sqlalchemy import create_engine
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json

load_dotenv()

DB_RELACIONAL_PATH = 'datos_paises.db' 
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://giovannisantos1890:<>@cluster0.vqry3kh.mongodb.net/?retryWrites=true&w=majority")

MONGO_DB = "paisesDB"
MONGO_COLLECTIONS = ["big_mac_index", "costos_turisticos"]

def conectar_sqlite():
    try:
        engine = create_engine(f'sqlite:///{DB_RELACIONAL_PATH}')
        print(f"Conexión establecida con la base de datos SQLite: {DB_RELACIONAL_PATH}")
        return engine
    except Exception as e:
        print(f"Error al conectar con SQLite: {e}")
        return None

def conectar_mongodb():
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        print("Conexión establecida con MongoDB Atlas")
        return client
    except Exception as e:
        print(f"Error al conectar con MongoDB Atlas: {e}")
        return None

def extraer_datos_relacionales(engine):
    try:
        query_envejecimiento = "SELECT * FROM pais_envejecimiento"
        df_envejecimiento = pd.read_sql(query_envejecimiento, engine)
        print(f"Datos extraídos de la pais_envejecimiento: {len(df_envejecimiento)} registros")
        
        query_poblacion = "SELECT * FROM pais_poblacion"
        df_poblacion = pd.read_sql(query_poblacion, engine)
        print(f"Datos extraídos de pais_poblacion: {len(df_poblacion)} registros")
        
        return df_envejecimiento, df_poblacion
    except Exception as e:
        print(f"Error al extraer datos relacionales: {e}")
        return None, None

def extraer_datos_mongodb(client):
    try:
        db = client[MONGO_DB]
        
        big_mac_data = list(db["big_mac_index"].find())
        df_big_mac = pd.DataFrame(big_mac_data)
        print(f"Datos extraídos de la colección big_mac_index: {len(df_big_mac)} registros")
        
        costos_data = list(db["costos_turisticos"].find())
        df_costos = pd.DataFrame(costos_data)
        print(f"Datos extraídos de la colección costos_turisticos: {len(df_costos)} registros")
        
        return df_big_mac, df_costos
    except Exception as e:
        print(f"Error al extraer datos de MongoDB: {e}")
        return None, None

def normalizar_nombres_paises(df, columna_pais):
    if columna_pais not in df.columns:
        print(f"La columna '{columna_pais}' no existe en el DataFrame")
        return df
    
    df[f"{columna_pais}_norm"] = df[columna_pais].str.lower()
    
    reemplazos = {
        'united states': 'estados unidos',
        'united states of america': 'estados unidos',
        'usa': 'estados unidos',
        'united kingdom': 'reino unido',
        'uk': 'reino unido',
        'czech republic': 'república checa',
        'russia': 'rusia',
        'vatican city': 'ciudad del vaticano'
    }
    
    df[f"{columna_pais}_norm"] = df[f"{columna_pais}_norm"].replace(reemplazos)
    
    return df

def preparar_dataframes(df_envejecimiento, df_poblacion, df_big_mac, df_costos):

    df_envejecimiento = normalizar_nombres_paises(df_envejecimiento, 'nombre_pais')
    df_poblacion = normalizar_nombres_paises(df_poblacion, 'pais')
    df_big_mac = normalizar_nombres_paises(df_big_mac, 'pais')
    df_costos = normalizar_nombres_paises(df_costos, 'pais')
    
    if '_id' in df_big_mac.columns:
        df_big_mac = df_big_mac.drop('_id', axis=1)
    if '_id' in df_costos.columns:
        df_costos = df_costos.drop('_id', axis=1)
    
    df_envejecimiento = df_envejecimiento.rename(columns={
        'poblacion': 'poblacion_env'
    })
    
    df_poblacion = df_poblacion.rename(columns={
        'continente': 'continente_pob',
        'poblacion': 'poblacion_pob'
    })
    
    if 'costos' in df_costos.columns and df_costos['costos'].notna().any():
        for registro in df_costos.itertuples():
            if pd.notna(registro.costos) and isinstance(registro.costos, dict):
                for clave, valor in registro.costos.items():
                    if clave not in df_costos.columns:
                        df_costos[clave] = None
                    df_costos.at[registro.Index, clave] = valor
        
        df_costos = df_costos.drop('costos', axis=1)
    
    return df_envejecimiento, df_poblacion, df_big_mac, df_costos

def integrar_datos(df_envejecimiento, df_poblacion, df_big_mac, df_costos):
    print("\nIntegrando datos")
    
    print("Integrando datos relacionales...")
    df_relacional = pd.merge(
        df_envejecimiento,
        df_poblacion,
        left_on='nombre_pais_norm',
        right_on='pais_norm',
        how='outer',
        suffixes=('_env', '_pob')
    )
    print(f"Datos relacionales integrados: {len(df_relacional)} registros")
    
    print("Integrando datos de Big Mac.")
    df_con_big_mac = pd.merge(
        df_relacional,
        df_big_mac,
        left_on='nombre_pais_norm',
        right_on='pais_norm',
        how='outer',
        suffixes=('_rel', '_big_mac')
    )
    print(f"✓ Datos con Big Mac integrados: {len(df_con_big_mac)} registros")
    
    print("Integrando datos de costos turísticos.")
    df_integrado = pd.merge(
        df_con_big_mac,
        df_costos,
        left_on='nombre_pais_norm',
        right_on='pais_norm',
        how='outer',
        suffixes=('_previo', '_costos')
    )
    print(f"✓ Todos los datos integrados: {len(df_integrado)} registros")
    
    return df_integrado

def limpiar_datos_integrados(df_integrado):
    """Limpia y consolida los datos integrados"""
    print("\nLimpiando y consolidando datos")
    
    columnas_pais = [col for col in df_integrado.columns if 'pais' in col.lower() and '_norm' not in col]
    columnas_continente = [col for col in df_integrado.columns if 'continente' in col.lower()]
    columnas_poblacion = [col for col in df_integrado.columns if 'poblacion' in col.lower()]
    
    print(f"Columnas de país: {columnas_pais}")
    print(f"Columnas de continente: {columnas_continente}")
    print(f"Columnas de población: {columnas_poblacion}")
    
    df_limpio = df_integrado.copy()
    
    if 'nombre_pais' in df_limpio.columns:
        df_limpio['pais'] = df_limpio['nombre_pais']
    elif 'pais_previo' in df_limpio.columns:
        df_limpio['pais'] = df_limpio['pais_previo']
    elif 'pais' in df_limpio.columns:
        pass  
    else:

        for col in columnas_pais:
            df_limpio['pais'] = df_limpio[col]
            break
    
    if 'continente' in df_limpio.columns:
        pass 
    elif 'continente_pob' in df_limpio.columns:
        df_limpio['continente'] = df_limpio['continente_pob']
    elif 'continente_previo' in df_limpio.columns:
        df_limpio['continente'] = df_limpio['continente_previo']
    elif 'continente_costos' in df_limpio.columns:
        df_limpio['continente'] = df_limpio['continente_costos']
    else:
        for col in columnas_continente:
            df_limpio['continente'] = df_limpio[col]
            break
    
    if 'poblacion_env' in df_limpio.columns and 'poblacion_pob' in df_limpio.columns:
        mascara = df_limpio['poblacion_env'].notna() & df_limpio['poblacion_pob'].notna()
        df_limpio.loc[mascara, 'poblacion'] = ((df_limpio.loc[mascara, 'poblacion_env'] + 
                                             df_limpio.loc[mascara, 'poblacion_pob']) / 2).astype('float64')
        
        mascara_env = df_limpio['poblacion_env'].notna() & df_limpio['poblacion_pob'].isna()
        df_limpio.loc[mascara_env, 'poblacion'] = df_limpio.loc[mascara_env, 'poblacion_env'].astype('float64')
        
        mascara_pob = df_limpio['poblacion_env'].isna() & df_limpio['poblacion_pob'].notna()
        df_limpio.loc[mascara_pob, 'poblacion'] = df_limpio.loc[mascara_pob, 'poblacion_pob'].astype('float64')
    elif 'poblacion' in df_limpio.columns:
        df_limpio['poblacion'] = df_limpio['poblacion'].astype('float64')  
    elif 'poblacion_env' in df_limpio.columns:
        df_limpio['poblacion'] = df_limpio['poblacion_env'].astype('float64') 
    elif 'poblacion_pob' in df_limpio.columns:
        df_limpio['poblacion'] = df_limpio['poblacion_pob'].astype('float64')  
    
    if 'capital_previo' in df_limpio.columns:
        df_limpio['capital'] = df_limpio['capital_previo']
    elif 'capital_costos' in df_limpio.columns:
        df_limpio['capital'] = df_limpio['capital_costos']
    
    columnas_finales = [
        'id_pais', 'pais', 'capital', 'continente', 'region', 'poblacion', 
        'tasa_de_envejecimiento', 'precio_big_mac_usd'
    ]
    
    columnas_costos = [col for col in df_limpio.columns if 'costo' in col.lower() and '_' in col]
    columnas_finales.extend(columnas_costos)
    
    columnas_existentes = [col for col in columnas_finales if col in df_limpio.columns]
    
    df_final = df_limpio[columnas_existentes].copy()
    
    for col in df_final.select_dtypes(include=['object']).columns:
        df_final[col] = df_final[col].fillna('Desconocido')
    
    for col in df_final.select_dtypes(include=['float', 'int']).columns:
        if col == 'id_pais':
            if df_final[col].isna().any():
                max_id = df_final[col].max()
                if pd.isna(max_id):
                    max_id = 0
                mascara_nulos = df_final[col].isna()
                df_final.loc[mascara_nulos, col] = range(int(max_id) + 1, int(max_id) + 1 + mascara_nulos.sum())
        elif 'precio' in col or 'costo' in col or 'tasa' in col:
            media = df_final[col].mean()
            if pd.isna(media):
                media = 0
            df_final[col] = df_final[col].fillna(media)
        else:
            df_final[col] = df_final[col].fillna(0)
    
    print(f"\nDatos consolidados: {len(df_final)} registros con {len(df_final.columns)} columnas")
    
    print("\nColumnas del DataFrame final:")
    for col in df_final.columns:
        tipo = df_final[col].dtype
        nulos = df_final[col].isna().sum()
        print(f"- {col} ({tipo}): {nulos} valores nulos")
    
    print("\nEjemplos de registros integrados:")
    muestra = df_final.sample(min(3, len(df_final))).reset_index(drop=True)
    print(muestra)
    
    return df_final

def guardar_datos_integrados(df_final):
    try:
        archivo_salida = "datos_integrados.csv"
        df_final.to_csv(archivo_salida, index=False)
        print(f"Datos integrados guardados en: {archivo_salida}")
        return True
    except Exception as e:
        print(f"Error al guardar los datos: {e}")
        return False
    
def mostrar_estadisticas(df_final):
    print("\nEstadísticas de los datos integrados")
    
    print("\nDistribución por continente:")
    if 'continente' in df_final.columns:
        continentes = df_final['continente'].value_counts()
        for continente, count in continentes.items():
            print(f"- {continente}: {count} países")
    
    if 'poblacion' in df_final.columns:
        print("\nEstadísticas de población:")
        print(f"- Población total: {df_final['poblacion'].sum():,.0f}")
        print(f"- Población media: {df_final['poblacion'].mean():,.0f}")
        print(f"- País más poblado: {df_final.loc[df_final['poblacion'].idxmax(), 'pais']} "
              f"({df_final['poblacion'].max():,.0f})")
        print(f"- País menos poblado: {df_final.loc[df_final['poblacion'].idxmin(), 'pais']} "
              f"({df_final['poblacion'].min():,.0f})")
    
    if 'precio_big_mac_usd' in df_final.columns:
        print("\nEstadísticas de precios Big Mac:")
        print(f"- Precio medio: ${df_final['precio_big_mac_usd'].mean():.2f}")
        print(f"- País más caro: {df_final.loc[df_final['precio_big_mac_usd'].idxmax(), 'pais']} "
              f"(${df_final['precio_big_mac_usd'].max():.2f})")
        print(f"- País más barato: {df_final.loc[df_final['precio_big_mac_usd'].idxmin(), 'pais']} "
              f"(${df_final['precio_big_mac_usd'].min():.2f})")
    
    if 'tasa_de_envejecimiento' in df_final.columns:
        print("\nEstadísticas de tasa de envejecimiento:")
        print(f"- Tasa media: {df_final['tasa_de_envejecimiento'].mean():.2f}%")
        print(f"- País con mayor tasa: {df_final.loc[df_final['tasa_de_envejecimiento'].idxmax(), 'pais']} "
              f"({df_final['tasa_de_envejecimiento'].max():.2f}%)")
        print(f"- País con menor tasa: {df_final.loc[df_final['tasa_de_envejecimiento'].idxmin(), 'pais']} "
              f"({df_final['tasa_de_envejecimiento'].min():.2f}%)")

def main():
    print("Ejercicio 2.3\n")
    
    engine_sqlite = conectar_sqlite()
    if not engine_sqlite:
        print("No se pudo conectar a SQL")
        return
    
    client_mongo = conectar_mongodb()
    if not client_mongo:
        print("No se pudo conectar a MongoDB")
        return
    
    df_envejecimiento, df_poblacion = extraer_datos_relacionales(engine_sqlite)
    if df_envejecimiento is None or df_poblacion is None:
        print("Error al extraer datos relacionales.")
        return
    
    df_big_mac, df_costos = extraer_datos_mongodb(client_mongo)
    if df_big_mac is None or df_costos is None:
        print("Error al extraer datos de MongoDB.")
        return
    
    df_envejecimiento, df_poblacion, df_big_mac, df_costos = preparar_dataframes(
        df_envejecimiento, df_poblacion, df_big_mac, df_costos
    )
    
    df_integrado = integrar_datos(df_envejecimiento, df_poblacion, df_big_mac, df_costos)
    
    df_final = limpiar_datos_integrados(df_integrado)
    
    guardar_datos_integrados(df_final)
    
    mostrar_estadisticas(df_final)
    
    engine_sqlite.dispose()
    client_mongo.close()
    
    print("\nIntegracion completada.")

if __name__ == "__main__":
    main()