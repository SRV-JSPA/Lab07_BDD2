import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

json_files = [
    'paises_mundo_big_mac.json',
    'costos_turisticos_africa.json',
    'costos_turisticos_america.json',
    'costos_turisticos_asia.json',
    'costos_turisticos_europa.json'
]

def conectar_mongodb():
    MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://giovannisantos1890:<>@cluster0.vqry3kh.mongodb.net/")
    
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        print("Conexión establecida con MongoDB Atlas")
        return client
    except Exception as e:
        print(f"Error al conectar con MongoDB Atlas: {e}")
        return None

def cargar_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            print(f"Archivo cargado: {file_path}")
            return data
    except Exception as e:
        print(f"Error al cargar el archivo {file_path}: {e}")
        return None

def analizar_datos(data, file_name):
    if not data:
        return
    print(f"\nAnálisis de {file_name} ---")
    print(f"Número de registros: {len(data)}")
    
    muestra = data[0] if data else {}
    
    print("\nEstructura del documento:")
    for key, value in muestra.items():
        if isinstance(value, dict):
            print(f"- {key}: {type(value).__name__} con claves {list(value.keys())}")
        else:
            print(f"- {key}: {type(value).__name__}")
    
    if 'país' in muestra or 'pais' in muestra:
        key_pais = 'país' if 'país' in muestra else 'pais'
        paises = [item[key_pais] for item in data if key_pais in item]
        print(f"\nPaíses representados: {len(set(paises))}")
        print(f"Ejemplos: {', '.join(list(set(paises))[:5])}")
    
    if 'continente' in muestra:
        continentes = [item['continente'] for item in data if 'continente' in item]
        print(f"\nContinentes representados: {', '.join(set(continentes))}")
    
    return

def unificar_estructura(data, file_name):
    if not data:
        return []
    
    resultado = []
    
    if file_name == 'paises_mundo_big_mac.json':
        for item in data:
            nuevo_item = {
                'pais': item.get('país', ''),
                'continente': item.get('continente', ''),
                'precio_big_mac_usd': item.get('precio_big_mac_usd', 0),
                'tipo_dato': 'big_mac'
            }
            resultado.append(nuevo_item)
    else:
        for item in data:
            costos = {}
            if 'costos_diarios_estimados_en_dólares' in item:
                costos_data = item['costos_diarios_estimados_en_dólares']
                
                for categoria, valores in costos_data.items():
                    if isinstance(valores, dict):
                        for tipo, valor in valores.items():
                            costos[f"{categoria}_{tipo}"] = valor
                    else:
                        costos[categoria] = valores
            
            nuevo_item = {
                'pais': item.get('país', item.get('pais', '')),
                'continente': item.get('continente', ''),
                'poblacion': item.get('población', item.get('poblacion', 0)),
                'capital': item.get('capital', ''),
                'region': item.get('región', item.get('region', '')),
                'costos': costos,
                'tipo_dato': 'costos_turisticos',
                'fuente': file_name
            }
            resultado.append(nuevo_item)
    
    print(f"Estructura unificada para {file_name}: {len(resultado)} registros")
    return resultado

def verificar_valores_nulos(data):
    if not data:
        return data
    
    df = pd.DataFrame(data)
    
    nulos = df.isnull().sum()
    if nulos.sum() > 0:
        print("\nValores nulos encontrados:")
        print(nulos[nulos > 0])
        
        print("\nRealizando limpieza de valores nulos...")
        
        df_limpio = df.fillna({
            'pais': 'Desconocido',
            'continente': 'Desconocido',
            'capital': 'Desconocido',
            'region': 'Desconocido'
        })
        
        for col in df.select_dtypes(include=['float64', 'int64']).columns:
            df_limpio[col] = df_limpio[col].fillna(0)
        
        return df_limpio.to_dict('records')
    
    return data

def cargar_en_mongodb(client, data, collection_name):
    if not client or not data:
        return False
    
    try:
        db = client['paisesDB']
        collection = db[collection_name]
        
        result = collection.delete_many({})
        if result.deleted_count > 0:
            print(f"- {result.deleted_count} documentos eliminados de la colección existente.")
        
        result = collection.insert_many(data)
        print(f"✓ {len(result.inserted_ids)} documentos insertados en la colección {collection_name}.")
        
        collection.create_index("pais")
        collection.create_index("continente")
        print(f"Indices creados en 'pais' y 'continente' para la colección {collection_name}.")
        
        return True
    except Exception as e:
        print(f"Error al cargar datos: {e}")
        return False

def realizar_consultas_prueba(client):
    if not client:
        return
    
    db = client['paisesDB']
    
    print("Consultas de prueba ---")
    
    colecciones = ['big_mac_index', 'costos_turisticos']
    for col in colecciones:
        count = db[col].count_documents({})
        print(f"- Colección {col}: {count} documentos")
    
    print("\nDistribución de países por continente (costos turísticos):")
    pipeline = [
        {"$group": {"_id": "$continente", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for result in db['costos_turisticos'].aggregate(pipeline):
        print(f"  - {result['_id']}: {result['count']} países")
    
    print("\nEjemplos de precios de Big Mac:")
    for doc in db['big_mac_index'].find().limit(3):
        print(f"  - {doc['pais']}: ${doc['precio_big_mac_usd']}")
    
    paises_big_mac = set([doc['pais'] for doc in db['big_mac_index'].find({}, {"pais": 1})])
    paises_costos = set([doc['pais'] for doc in db['costos_turisticos'].find({}, {"pais": 1})])
    paises_comunes = paises_big_mac.intersection(paises_costos)
    
    print(f"\nPaíses presentes en ambas colecciones: {len(paises_comunes)}")
    print(f"Ejemplos: {', '.join(list(paises_comunes)[:5])}")

def main():
    print("Ejercicio 2.2\n")
    
    client = conectar_mongodb()
    if not client:
        print("No se pudo conectar con MongoDB. Abortando.")
        return
    
    datos_big_mac = []
    datos_costos = []
    
    for file_name in json_files:
        data = cargar_json(file_name)
        if data:
            analizar_datos(data, file_name)
            
            datos_unificados = unificar_estructura(data, file_name)
            
            if file_name == 'paises_mundo_big_mac.json':
                datos_big_mac.extend(datos_unificados)
            else:
                datos_costos.extend(datos_unificados)
    
    datos_big_mac = verificar_valores_nulos(datos_big_mac)
    datos_costos = verificar_valores_nulos(datos_costos)
    
    print("\n--- Cargando datos en MongoDB ---")
    cargar_en_mongodb(client, datos_big_mac, 'big_mac_index')
    cargar_en_mongodb(client, datos_costos, 'costos_turisticos')
    
    realizar_consultas_prueba(client)
    
    client.close()
    print("\Proceso completado.")

if __name__ == "__main__":
    main()