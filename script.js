//Ejercicio 2.2 
const crearTablasEnvejecimiento = `
    CREATE TABLE pais_envejecimiento (
        id_pais INTEGER PRIMARY KEY,
        nombre_pais TEXT NOT NULL,
        capital TEXT,
        continente TEXT,
        region TEXT,
        poblacion REAL,
        tasa_de_envejecimiento REAL
    )
`;
const crearTablasPoblacion = `
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
`;
//Ejercicio 2.4
const creatTablasdim_pais = `
    CREATE TABLE dim_pais (
        id_pais INTEGER PRIMARY KEY,
        pais TEXT NOT NULL,
        capital TEXT,
        continente TEXT,
        region TEXT,
        poblacion REAL,
        tasa_de_envejecimiento REAL
    )
`;
const creatTablasdim_costos  = `
    CREATE TABLE dim_costos (
        id_costo INTEGER PRIMARY KEY,
        tipo_costo TEXT NOT NULL,
        descripcion TEXT
    )
`;
const creatTablasdim_tiempo  = `
    CREATE TABLE dim_tiempo (
        id_tiempo INTEGER PRIMARY KEY,
        fecha_carga TEXT NOT NULL,
        anio INTEGER,
        mes INTEGER,
        dia INTEGER
    )
`;
const creatTablasfact_economicos  = `
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
`;
//Ejercicio 3
    const queryBigMac = `
    SELECT p.pais, p.continente, f.valor
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo = 'big_mac'
    ORDER BY f.valor DESC
    LIMIT 5
    `;

    const queryHospedaje = `
    SELECT p.continente, AVG(f.valor) as promedio
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo = 'hospedaje'
    GROUP BY p.continente
    ORDER BY promedio DESC
    `;

    const queryEnvejecimiento = `
    SELECT p.pais, p.continente, f.valor as precio_big_mac, p.tasa_de_envejecimiento
    FROM fact_economicos f
    JOIN dim_pais p ON f.id_pais = p.id_pais
    JOIN dim_costos c ON f.id_costo = c.id_costo
    WHERE c.tipo_costo = 'big_mac'
    ORDER BY f.valor DESC
    LIMIT 10
    `;

    const queryCostosTuristicos = `
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
    `;
    const queryPaisesEconomicos = `
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
    `;

    const queryPoblacion = `
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
    `;
