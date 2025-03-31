import pandas as pd
import sqlite3
import os

def crear_base_datos():
    db_path = 'datos_paises.db'
    
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Base de datos anterior eliminada: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Base de datos creada: {db_path}")
    return conn, cursor

def crear_tablas(conn, cursor):
    cursor.execute('''
    CREATE TABLE pais_envejecimiento (
        id_pais INTEGER PRIMARY KEY,
        nombre_pais TEXT NOT NULL,
        capital TEXT,
        continente TEXT,
        region TEXT,
        poblacion REAL,
        tasa_de_envejecimiento REAL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE pais_poblacion (
        _id TEXT PRIMARY KEY,
        continente TEXT,
        pais TEXT NOT NULL,
        poblacion INTEGER,
        costo_bajo_hospedaje REAL,
        costo_promedio_comida REAL,
        costo_bajo_transporte REAL,
        costo_promedio_entretenimiento REAL
    )
    ''')
    
    conn.commit()
    print("Tablas creadas: pais_envejecimiento, pais_poblacion")

def cargar_datos(conn, cursor):
    try:
        df_envejecimiento = pd.read_csv('pais_envejecimiento.csv')
        df_poblacion = pd.read_csv('pais_poblacion.csv')
        
        print("\nDatos cargados desde CSV:")
        print(f"- pais_envejecimiento: {len(df_envejecimiento)} registros")
        print(f"- pais_poblacion: {len(df_poblacion)} registros")
        
        nulos_env = df_envejecimiento.isnull().sum()
        nulos_pob = df_poblacion.isnull().sum()
        
        print("\nAnálisis de valores nulos en pais_envejecimiento:")
        print(nulos_env)
        
        print("\nAnálisis de valores nulos en pais_poblacion:")
        print(nulos_pob)
        
        if nulos_env.sum() > 0:
            print("\nLimpiando valores nulos en pais_envejecimiento...")
            for col in ['capital', 'continente', 'region']:
                if nulos_env[col] > 0:
                    df_envejecimiento[col] = df_envejecimiento[col].fillna('Desconocido')
                    print(f"  - {nulos_env[col]} valores nulos en '{col}' rellenados con 'Desconocido'")
            
            for col in ['poblacion', 'tasa_de_envejecimiento']:
                if nulos_env[col] > 0:
                    media = df_envejecimiento[col].mean()
                    df_envejecimiento[col] = df_envejecimiento[col].fillna(media)
                    print(f"  - {nulos_env[col]} valores nulos en '{col}' rellenados con la media: {media:.2f}")
        
        if nulos_pob.sum() > 0:
            print("\nLimpiando valores nulos en pais_poblacion...")
            for col in ['_id', 'continente', 'pais']:
                if nulos_pob[col] > 0:
                    df_poblacion[col] = df_poblacion[col].fillna('Desconocido')
                    print(f"  - {nulos_pob[col]} valores nulos en '{col}' rellenados con 'Desconocido'")
            
            cols_num = ['poblacion', 'costo_bajo_hospedaje', 'costo_promedio_comida', 
                         'costo_bajo_transporte', 'costo_promedio_entretenimiento']
            for col in cols_num:
                if nulos_pob[col] > 0:
                    media = df_poblacion[col].mean()
                    df_poblacion[col] = df_poblacion[col].fillna(media)
                    print(f"  - {nulos_pob[col]} valores nulos en '{col}' rellenados con la media: {media:.2f}")
        
        df_envejecimiento.to_sql('pais_envejecimiento', conn, if_exists='replace', index=False)
        df_poblacion.to_sql('pais_poblacion', conn, if_exists='replace', index=False)
        
        print("\nDatos cargados en la base de datos SQLite.")
        
    except Exception as e:
        print(f"Error al cargar los datos: {e}")

def verificar_carga(cursor):
    print("\nVerificando datos cargados:")
    
    cursor.execute("SELECT COUNT(*) FROM pais_envejecimiento")
    count = cursor.fetchone()[0]
    print(f"- pais_envejecimiento: {count} registros")
    
    cursor.execute("SELECT * FROM pais_envejecimiento LIMIT 3")
    print("\nEjemplos de pais_envejecimiento:")
    cols = [desc[0] for desc in cursor.description]
    print(cols)
    for row in cursor.fetchall():
        print(row)
    
    cursor.execute("SELECT COUNT(*) FROM pais_poblacion")
    count = cursor.fetchone()[0]
    print(f"\n- pais_poblacion: {count} registros")
    
    cursor.execute("SELECT * FROM pais_poblacion LIMIT 3")
    print("\nEjemplos de pais_poblacion:")
    cols = [desc[0] for desc in cursor.description]
    print(cols)
    for row in cursor.fetchall():
        print(row)


def main():
    print("Creacion de base de datos\n")
    
    conn, cursor = crear_base_datos()
    
    crear_tablas(conn, cursor)
    
    cargar_datos(conn, cursor)
    
    verificar_carga(cursor)
        
    conn.close()
    
    print(f"Base de datos creada")

if __name__ == "__main__":
    main()