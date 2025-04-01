import pandas as pd
import sqlite3
import os
from datetime import datetime

def crear_datawarehouse():
    db_path = 'data_warehouse.db'
    
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Base de datos anterior eliminada: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE dim_pais (
        id_pais INTEGER PRIMARY KEY,
        pais TEXT NOT NULL,
        capital TEXT,
        continente TEXT,
        region TEXT,
        poblacion REAL,
        tasa_de_envejecimiento REAL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE dim_costos (
        id_costo INTEGER PRIMARY KEY,
        tipo_costo TEXT NOT NULL,
        descripcion TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE dim_tiempo (
        id_tiempo INTEGER PRIMARY KEY,
        fecha_carga TEXT NOT NULL,
        anio INTEGER,
        mes INTEGER,
        dia INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE fact_economicos (
        id_hecho INTEGER PRIMARY KEY AUTOINCREMENT,
        id_pais INTEGER,
        id_costo INTEGER,
        id_tiempo INTEGER,
        valor REAL,
        FOREIGN KEY (id_pais) REFERENCES dim_pais (id_pais),
        FOREIGN KEY (id_costo) REFERENCES dim_costos (id_costo),
        FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo (id_tiempo)
    )
    ''')
    
    conn.commit()
    print(f"Base de datos data warehouse creada: {db_path}")
    return conn, cursor

def cargar_dimension_costos(cursor):
    tipos_costos = [
        (1, 'big_mac', 'Precio del Big Mac en USD'),
        (2, 'hospedaje', 'Costo bajo de hospedaje'),
        (3, 'comida', 'Costo promedio de comida'),
        (4, 'transporte', 'Costo bajo de transporte'),
        (5, 'entretenimiento', 'Costo promedio de entretenimiento')
    ]
    
    cursor.executemany('''
    INSERT INTO dim_costos (id_costo, tipo_costo, descripcion)
    VALUES (?, ?, ?)
    ''', tipos_costos)
    
    print(f"Dimensión costos cargada con {len(tipos_costos)} registros")

def cargar_dimension_tiempo(cursor):
    ahora = datetime.now()
    tiempo = (1, ahora.strftime('%Y-%m-%d'), ahora.year, ahora.month, ahora.day)
    
    cursor.execute('''
    INSERT INTO dim_tiempo (id_tiempo, fecha_carga, anio, mes, dia)
    VALUES (?, ?, ?, ?, ?)
    ''', tiempo)
    
    print(f"Dimensión tiempo cargada con fecha: {tiempo[1]}")
    return tiempo[0] 

def cargar_datos_integrados(conn, cursor, id_tiempo):
    try:
        df = pd.read_csv('datos_integrados.csv')
        print(f"Datos cargados desde CSV: {len(df)} registros")
        
        for _, row in df.iterrows():
            cursor.execute('''
            INSERT OR IGNORE INTO dim_pais (id_pais, pais, capital, continente, region, poblacion, tasa_de_envejecimiento)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row['id_pais']),
                row['pais'],
                row['capital'],
                row['continente'],
                row.get('region_costos', 'Desconocido'),
                row['poblacion'],
                row['tasa_de_envejecimiento']
            ))
        
        conn.commit()
        print(f"Dimensión país cargada con {len(df)} registros")
        
        hechos = []
        for _, row in df.iterrows():
            if pd.notna(row['precio_big_mac_usd']):
                hechos.append((
                    int(row['id_pais']),
                    1,  
                    id_tiempo,
                    row['precio_big_mac_usd']
                ))
            
            if 'costo_bajo_hospedaje' in row and pd.notna(row['costo_bajo_hospedaje']):
                hechos.append((
                    int(row['id_pais']),
                    2,  
                    id_tiempo,
                    row['costo_bajo_hospedaje']
                ))
            
            if 'costo_promedio_comida' in row and pd.notna(row['costo_promedio_comida']):
                hechos.append((
                    int(row['id_pais']),
                    3,  
                    id_tiempo,
                    row['costo_promedio_comida']
                ))
            
            if 'costo_bajo_transporte' in row and pd.notna(row['costo_bajo_transporte']):
                hechos.append((
                    int(row['id_pais']),
                    4,  
                    id_tiempo,
                    row['costo_bajo_transporte']
                ))
            
            if 'costo_promedio_entretenimiento' in row and pd.notna(row['costo_promedio_entretenimiento']):
                hechos.append((
                    int(row['id_pais']),
                    5,  
                    id_tiempo,
                    row['costo_promedio_entretenimiento']
                ))
        
        cursor.executemany('''
        INSERT INTO fact_economicos (id_pais, id_costo, id_tiempo, valor)
        VALUES (?, ?, ?, ?)
        ''', hechos)
        
        conn.commit()
        print(f"Tabla de hechos cargada con {len(hechos)} registros")
        
        return True
    except Exception as e:
        print(f"Error al cargar los datos integrados: {e}")
        return False

def verificar_carga(cursor):
    print("\nVerificando datos cargados en el data warehouse:")
    
    cursor.execute("SELECT COUNT(*) FROM dim_pais")
    count = cursor.fetchone()[0]
    print(f"- dim_pais: {count} registros")
    
    cursor.execute("SELECT COUNT(*) FROM dim_costos")
    count = cursor.fetchone()[0]
    print(f"- dim_costos: {count} registros")
    
    cursor.execute("SELECT COUNT(*) FROM dim_tiempo")
    count = cursor.fetchone()[0]
    print(f"- dim_tiempo: {count} registros")
    
    cursor.execute("SELECT COUNT(*) FROM fact_economicos")
    count = cursor.fetchone()[0]
    print(f"- fact_economicos: {count} registros")
    
    print("\nTop 5 países con precio de Big Mac más alto:")
    cursor.execute('''
    SELECT p.pais, p.continente, f.valor
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo = 'big_mac'
    ORDER BY f.valor DESC
    LIMIT 5
    ''')
    
    for row in cursor.fetchall():
        print(f"- {row[0]} ({row[1]}): ${row[2]:.2f}")
    
    print("\nPromedio de costos de hospedaje por continente:")
    cursor.execute('''
    SELECT p.continente, AVG(f.valor) as promedio
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo = 'hospedaje'
    GROUP BY p.continente
    ORDER BY promedio DESC
    ''')
    
    for row in cursor.fetchall():
        print(f"- {row[0]}: ${row[1]:.2f}")

def realizar_analisis(cursor):
    print("\n--- ANÁLISIS DE DATOS DEL DATA WAREHOUSE ---")
    
    print("\n1. Países con alto precio de Big Mac y su tasa de envejecimiento:")
    cursor.execute('''
    SELECT p.pais, p.continente, f.valor as precio_big_mac, p.tasa_de_envejecimiento
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo = 'big_mac'
    ORDER BY f.valor DESC
    LIMIT 10
    ''')
    
    for row in cursor.fetchall():
        print(f"- {row[0]} ({row[1]}): Big Mac ${row[2]:.2f}, Envejecimiento {row[3]:.2f}%")
    
    print("\n2. Comparativa de costos turísticos por continente:")
    cursor.execute('''
    SELECT 
        p.continente,
        avg(CASE WHEN c.tipo_costo = 'hospedaje' THEN f.valor ELSE NULL END) as avg_hospedaje,
        avg(CASE WHEN c.tipo_costo = 'comida' THEN f.valor ELSE NULL END) as avg_comida,
        avg(CASE WHEN c.tipo_costo = 'transporte' THEN f.valor ELSE NULL END) as avg_transporte,
        avg(CASE WHEN c.tipo_costo = 'entretenimiento' THEN f.valor ELSE NULL END) as avg_entretenimiento
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    GROUP BY p.continente
    ORDER BY avg_hospedaje DESC
    ''')
    
    for row in cursor.fetchall():
        print(f"- {row[0]}:")
        print(f"  * Hospedaje: ${row[1]:.2f}")
        print(f"  * Comida: ${row[2]:.2f}")
        print(f"  * Transporte: ${row[3]:.2f}")
        print(f"  * Entretenimiento: ${row[4]:.2f}")
    
    print("\n3. Países más económicos para turistas:")
    cursor.execute('''
    SELECT 
        p.pais,
        p.continente,
        SUM(f.valor) as costo_total
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo IN ('hospedaje', 'comida', 'transporte', 'entretenimiento')
    GROUP BY p.pais, p.continente
    ORDER BY costo_total ASC
    LIMIT 10
    ''')
    
    for row in cursor.fetchall():
        print(f"- {row[0]} ({row[1]}): ${row[2]:.2f}")
    
    print("\n4. Relación entre población y precio de Big Mac:")
    cursor.execute('''
    SELECT 
        p.pais,
        p.poblacion,
        f.valor as precio_big_mac
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo = 'big_mac'
    AND p.poblacion > 0
    ORDER BY p.poblacion DESC
    LIMIT 10
    ''')
    
    for row in cursor.fetchall():
        print(f"- {row[0]}: Población {row[1]:,.0f}, Big Mac ${row[2]:.2f}")

def main():
    print("Ejercicio 2.4\n")
    
    conn, cursor = crear_datawarehouse()
    
    cargar_dimension_costos(cursor)
    id_tiempo = cargar_dimension_tiempo(cursor)
    
    exito = cargar_datos_integrados(conn, cursor, id_tiempo)
    
    if exito:
        verificar_carga(cursor)
        realizar_analisis(cursor)
    
    conn.close()
    
    print("\nProceso de carga en el Data Warehouse completado.")

if __name__ == "__main__":
    main()