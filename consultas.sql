-- ==============================================================================
-- PROYECTO ETL PELÍCULAS (APACHE HOP + MYSQL + PARQUET + POWER BI)
-- Script: consultas.sql
-- Base de Datos: imdb
-- Tabla Principal: imdb_clean_final
-- Descripción: Conjunto completo de consultas SQL analíticas, de validación y
--              reporte de negocio para la tabla de películas procesada por el ETL.
-- ==============================================================================

USE imdb;

-- ==============================================================================
-- 1. VALIDACIÓN DE CALIDAD DE DATOS E INTEGRIDAD DEL PIPELINE
-- ==============================================================================

-- 1.1 Conteo total de registros depurados
-- Verifica que el ETL haya cargado el total esperado (~334,067 películas).
SELECT 
    COUNT(*) AS total_peliculas_cargadas,
    MIN(startYear) AS anio_mas_antiguo,
    MAX(startYear) AS anio_mas_reciente
FROM imdb_clean_final;

-- 1.2 Verificación de integridad: ausencia de nulos en columnas obligatorias
SELECT 
    SUM(CASE WHEN tconst IS NULL OR TRIM(tconst) = '' THEN 1 ELSE 0 END) AS nulos_tconst,
    SUM(CASE WHEN title IS NULL OR TRIM(title) = '' THEN 1 ELSE 0 END) AS nulos_title,
    SUM(CASE WHEN startYear IS NULL THEN 1 ELSE 0 END) AS nulos_anio,
    SUM(CASE WHEN genres IS NULL OR TRIM(genres) = '' THEN 1 ELSE 0 END) AS nulos_generos,
    SUM(CASE WHEN averageRating IS NULL THEN 1 ELSE 0 END) AS nulos_rating,
    SUM(CASE WHEN numVotes IS NULL THEN 1 ELSE 0 END) AS nulos_votos
FROM imdb_clean_final;

-- 1.3 Detección de posibles IDs duplicados (debe retornar 0 filas)
SELECT 
    tconst, 
    COUNT(*) AS duplicados
FROM imdb_clean_final
GROUP BY tconst
HAVING COUNT(*) > 1;


-- ==============================================================================
-- 2. MÉTRICAS EJECUTIVAS Y KPIS GLOBALES
-- ==============================================================================

-- 2.1 Resumen general del catálogo analítico
SELECT 
    COUNT(*) AS total_peliculas,
    ROUND(AVG(averageRating), 2) AS rating_promedio_global,
    ROUND(AVG(numVotes), 0) AS votos_promedio_por_pelicula,
    SUM(numVotes) AS volumen_total_votos,
    COUNT(DISTINCT startYear) AS total_anios_cubiertos
FROM imdb_clean_final;


-- ==============================================================================
-- 3. RANKINGS DE CALIDAD Y EXCELENCIA CINEMATOGRÁFICA
-- ==============================================================================

-- 3.1 Top 25 Películas Mejor Valoradas de la Historia
-- Se filtra por un mínimo de 100,000 votos para asegurar significancia estadística.
SELECT 
    tconst,
    title,
    startYear AS anio,
    genres AS generos,
    averageRating AS rating,
    FORMAT(numVotes, 0) AS total_votos
FROM imdb_clean_final
WHERE numVotes >= 100000
ORDER BY averageRating DESC, numVotes DESC
LIMIT 25;

-- 3.2 "Joyas Ocultas" del Cine
-- Películas con excelente valoración (>= 8.2) pero menor exposición mediática (10,000 a 60,000 votos).
SELECT 
    title,
    startYear AS anio,
    genres AS generos,
    averageRating AS rating,
    FORMAT(numVotes, 0) AS votos
FROM imdb_clean_final
WHERE averageRating >= 8.2
  AND numVotes BETWEEN 10000 AND 60000
ORDER BY averageRating DESC, numVotes DESC
LIMIT 15;

-- 3.3 Top 10 Clásicos de Oro (Antes de 1980)
SELECT 
    title,
    startYear AS anio,
    genres,
    averageRating AS rating,
    FORMAT(numVotes, 0) AS votos
FROM imdb_clean_final
WHERE startYear < 1980
  AND numVotes >= 50000
ORDER BY averageRating DESC
LIMIT 10;

-- 3.4 Top 10 Cine Contemporáneo (2010 en adelante)
SELECT 
    title,
    startYear AS anio,
    genres,
    averageRating AS rating,
    FORMAT(numVotes, 0) AS votos
FROM imdb_clean_final
WHERE startYear >= 2010
  AND numVotes >= 150000
ORDER BY averageRating DESC
LIMIT 10;


-- ==============================================================================
-- 4. ANÁLISIS TEMPORAL: DÉCADAS Y TENDENCIAS
-- ==============================================================================

-- 4.1 Evolución del Cine por Década
-- Analiza volumen de producción, calificación media y votos totales por década.
SELECT 
    FLOOR(startYear / 10) * 10 AS decada,
    COUNT(*) AS total_peliculas,
    ROUND(AVG(averageRating), 2) AS rating_promedio,
    ROUND(AVG(numVotes), 0) AS votos_promedio,
    FORMAT(SUM(numVotes), 0) AS votos_acumulados
FROM imdb_clean_final
WHERE startYear >= 1920
GROUP BY FLOOR(startYear / 10) * 10
ORDER BY decada ASC;

-- 4.2 Los Años Dorados del Cine (Años con mejor promedio y volumen significativo)
SELECT 
    startYear AS anio,
    COUNT(*) AS total_peliculas,
    ROUND(AVG(averageRating), 2) AS rating_promedio,
    ROUND(AVG(numVotes), 0) AS votos_promedio
FROM imdb_clean_final
GROUP BY startYear
HAVING COUNT(*) >= 500
ORDER BY rating_promedio DESC
LIMIT 10;


-- ==============================================================================
-- 5. ANÁLISIS DE GÉNEROS CINEMATOGRÁFICOS
-- ==============================================================================

-- 5.1 Las 15 combinaciones de géneros más frecuentes en el catálogo
SELECT 
    genres AS combinacion_generos,
    COUNT(*) AS cantidad_peliculas,
    ROUND(AVG(averageRating), 2) AS rating_promedio,
    FORMAT(SUM(numVotes), 0) AS votos_totales
FROM imdb_clean_final
GROUP BY genres
ORDER BY cantidad_peliculas DESC
LIMIT 15;

-- 5.2 Desempeño por Género Individual
-- Evalúa el volumen y la calidad promedio de los géneros clave.
SELECT 
    cat.genero,
    COUNT(*) AS total_peliculas,
    ROUND(AVG(f.averageRating), 2) AS rating_promedio,
    ROUND(AVG(f.numVotes), 0) AS votos_promedio
FROM (
    SELECT 'Drama' AS genero UNION ALL
    SELECT 'Comedy' UNION ALL
    SELECT 'Action' UNION ALL
    SELECT 'Thriller' UNION ALL
    SELECT 'Horror' UNION ALL
    SELECT 'Romance' UNION ALL
    SELECT 'Crime' UNION ALL
    SELECT 'Adventure' UNION ALL
    SELECT 'Sci-Fi' UNION ALL
    SELECT 'Animation' UNION ALL
    SELECT 'Documentary' UNION ALL
    SELECT 'Mystery'
) cat
JOIN imdb_clean_final f 
  ON FIND_IN_SET(cat.genero, REPLACE(f.genres, ', ', ',')) > 0
GROUP BY cat.genero
ORDER BY total_peliculas DESC;


-- ==============================================================================
-- 6. POPULARIDAD VS ACEPTACIÓN (VOLUMEN DE AUDIENCIA)
-- ==============================================================================

-- 6.1 Las 20 Películas Más Votadas / Populares de la Historia
SELECT 
    title,
    startYear AS anio,
    genres,
    FORMAT(numVotes, 0) AS total_votos,
    averageRating AS rating
FROM imdb_clean_final
ORDER BY numVotes DESC
LIMIT 20;

-- 6.2 Películas Altamente Votadas pero Mal Calificadas ("Blockbusters decepcionantes")
-- Tienen más de 100,000 votos pero la calificación es menor a 5.5.
SELECT 
    title,
    startYear AS anio,
    genres,
    FORMAT(numVotes, 0) AS total_votos,
    averageRating AS rating
FROM imdb_clean_final
WHERE numVotes >= 100000 
  AND averageRating < 5.5
ORDER BY numVotes DESC
LIMIT 10;


-- ==============================================================================
-- 7. CONSULTAS OPERATIVAS Y DE CRUCE (BÚSQUEDA Y POWER BI)
-- ==============================================================================

-- 7.1 Búsqueda rápida por coincidencia de título normalizado (title_key)
-- Ejemplo de búsqueda: 'batman'
SELECT 
    tconst,
    title,
    startYear,
    genres,
    averageRating,
    numVotes
FROM imdb_clean_final
WHERE title_key LIKE '%batman%'
ORDER BY startYear DESC, numVotes DESC
LIMIT 10;

-- 7.2 Vista de agregación lista para alimentar dashboards de Power BI
-- (Agrupa películas por década y género principal)
SELECT 
    FLOOR(startYear / 10) * 10 AS decada,
    SUBSTRING_INDEX(genres, ',', 1) AS genero_principal,
    COUNT(*) AS total_peliculas,
    ROUND(AVG(averageRating), 2) AS rating_promedio,
    SUM(numVotes) AS votos_totales
FROM imdb_clean_final
WHERE startYear >= 1950
GROUP BY FLOOR(startYear / 10) * 10, SUBSTRING_INDEX(genres, ',', 1)
ORDER BY decada DESC, total_peliculas DESC;
