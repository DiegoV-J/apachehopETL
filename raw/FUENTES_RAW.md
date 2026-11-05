# Fuentes RAW — Proyecto ETL Películas (Apache Hop + Power BI)

Carpeta: `C:\Users\diego\Downloads\apachehopETL\raw`
Descarga verificada: **2026-10-19** (integridad gzip/CSV comprobada y conteo de filas real).

## 1. Archivos principales (los que usa el pipeline)

| Archivo | Fuente | Tamaño | Filas | Columnas |
|---|---|---|---|---|
| `title.basics.tsv.gz` | https://datasets.imdbws.com/title.basics.tsv.gz | 228.8 MB | 12,876,214 | `tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres` |
| `title.ratings.tsv.gz` | https://datasets.imdbws.com/title.ratings.tsv.gz | 8.7 MB | 1,722,741 | `tconst, averageRating, numVotes` |
| `movies.csv` | MovieLens *ml-latest-small* (GroupLens) | 494 KB | 9,742 | `movieId, title, genres` |
| `ratings.csv` | MovieLens *ml-latest-small* (GroupLens) | 2.48 MB | 100,836 | `userId, movieId, rating, timestamp` |
| `boxoffice.csv` | GitHub `danielgrijalva/movie-stats` (dataset tipo Kaggle) | 1.34 MB | 7,668 | `name, rating, genre, year, released, score, votes, director, writer, star, country, budget, gross, company, runtime` |
| `boxoffice_mojo_daily.csv.gz` | GitHub `tjwaterman99/boxofficemojo-scraper`, release `2025-01-04` | 14.7 MB | 356,322 | `id, date, title, revenue, theaters, distributor` |

Origen exacto de los enlaces:

- IMDb: `https://datasets.imdbws.com/` (archivos oficiales, se actualizan a diario).
- MovieLens: `https://files.grouplens.org/datasets/movielens/ml-latest-small.zip` (978 KB).
- Box Office (opción GitHub): `https://github.com/tjwaterman99/boxofficemojo-scraper/releases/download/2025-01-04/revenues_per_day.csv.gz`

## 1b. MovieLens completo (`ml-latest`) — el que usa el pipeline

Descargado e instalado el 2026-10-19 en `raw\ml-latest\` (350.9 MB el zip, ~1.5 GB extraído).

| Archivo | Tamaño | Filas | Columnas |
|---|---|---|---|
| `ratings.csv` | 933.9 MB | **33,832,162** | `userId, movieId, rating, timestamp` |
| `movies.csv` | 4.2 MB | **86,537** | `movieId, title, genres` |
| `links.csv` | 1.9 MB | **86,537** | `movieId, imdbId, tmdbId` |
| `tags.csv` | 85.4 MB | — | `userId, movieId, tag, timestamp` |
| `genome-scores.csv` | 521.5 MB | — | vectores de afinidad de género (no usado) |
| `genome-tags.csv` | 18.1 KB | — | etiquetas del genoma |

- 330,975 usuarios distintos; valoraciones de **1995-01-09 a 2023-07-20**.
- `links.csv` tiene `imdbId` para **las 86,537 películas**, sin nulos → `tconst` = `'tt' + imdbId`.
- Sustituye a `raw\movies.csv` / `raw\ratings.csv` (que eran `ml-latest-small`, 100,836 valoraciones). Los pequeños se conservan por si quieres comparar.

**No** se descomprime nada a mano: el pipeline 02 lee `raw\ml-latest\movies.csv` y `links.csv` directamente.

## 2. Configuración exacta para los pasos de Hop

| Archivo | Paso Hop | Delimitador | Encoding | ¿Gzip? |
|---|---|---|---|---|
| `title.basics.tsv.gz` | Text file input | **Tab `\t`** | UTF-8 | Sí, nativo |
| `title.ratings.tsv.gz` | Text file input | **Tab `\t`** | UTF-8 | Sí, nativo |
| `movies.csv` | CSV file input | `,` | UTF-8 | No |
| `ratings.csv` | CSV file input | `,` | UTF-8 | No |
| `boxoffice.csv` | CSV file input | `,` (¡tiene campos entre comillas!) | **UTF-8 con BOM** | No |
| `boxoffice_mojo_daily.csv.gz` | Text/CSV file input | `,` | UTF-8 | Sí |

> Ojo con `boxoffice.csv`: la primera línea trae BOM (`\uFEFF`) y la columna `released` viene entre comillas porque contiene comas (`"June 13, 1980 (United States)"`). En el paso CSV marca **"File has a header row"** + **"Enclosed by \" "**; si no, la primera columna se llamará `﻿name` y el join fallará.

## 3. Calidad de datos ya medida (te sirve para justificar la limpieza)

**IMDb (`title.basics`)**
- Tipos: `tvEpisode` 9,958,819 · `short` 1,164,263 · **`movie` 759,906** · `video` 332,540 · `tvSeries` 307,235 · `tvMovie` 156,620.
- De las 759,906 películas: **113,722 (15.0 %) sin `startYear`** (`\N`) y **77,835 sin `genres`** (`\N`). Son los que debe eliminar el segundo *Filter rows*.
- `isAdult = 1` en 9,106 películas → conviene filtrarlas también.
- Solo 1,722,741 títulos tienen rating ⇒ el **INNER JOIN con ratings recorta mucho** (esperado).

**Box office (`boxoffice.csv`)**
- Rango de años: **1980–2020** (7,668 películas). No cubre catálogo antiguo ni estrenos posteriores a 2020.
- `gross` = **taquilla mundial en USD** (Avatar 2009 = 2,847,246,203; Titanic = 2,201,647,264). No es doméstica: nómbralo así en el dashboard.
- 189 filas sin `gross` y 2,171 sin `budget` → trata ambos como nulos, no como 0.
- **Homónimos reales**: `The Avengers` aparece como película de 1998 y `Parasite` como 1982. Unir solo por título duplica filas; **usa título + año**.

**MovieLens**
- `movies.csv` trae el año dentro del título (`Toy Story (1995)`) → hay que separarlo antes de comparar.
- Géneros separados por `|` y en inglés (`Adventure|Animation|Children`), distinto formato que IMDb (`Comedy,Drama`).

## 4. Atajo importante para el join: `other\links.csv`

`raw\other\ml-latest-small\links.csv` mapea `movieId → imdbId` para **las 9,742 películas, sin un solo nulo**.

Eso significa que MovieLens **no necesita limpieza de títulos**: `imdbId` + prefijo `tt` = `tconst` de IMDb (ej. `0114709` → `tt0114709`). Es un join exacto por ID, mucho más defendible en la exposición que un *fuzzy match* de strings. Deja la limpieza de títulos solo para el box office, que no tiene ID.

## 5. Extras ya presentes en `raw\other\` (opcionales, para la página 3 del dashboard)

| Archivo | Comprimido | Filas | Para qué |
|---|---|---|---|
| `name.basics.tsv.gz` | 311.7 MB | 15,749,712 | Nombres de directores/actores (rankings) |
| `title.crew.tsv.gz` | 83.7 MB | 12,880,486 | `tconst → directors, writers` (join directo con name.basics) |
| `title.principals.tsv.gz` | 787.5 MB | 102,392,013 | Reparto completo con personajes y orden |
| `title.episode.tsv.gz` | 55.1 MB | 9,958,819 | Episodios de series (no necesario si filtras `movie`) |
| `title.akas.tsv.gz` | 518.2 MB | 59,812,827 | Títulos por región/idioma (útil para match internacional) |
| `ml-latest-small.zip` | 978 KB | — | ZIP original de MovieLens |

Los cinco `.gz` se descomprimen sin errores (verificado leyendo el stream completo).

Con `title.crew.tsv.gz` + `name.basics.tsv.gz` sale la página de directores sin tocar `title.principals` (que es el archivo pesado).

## 6. Nota

`movies-dataset-analysis-and-box-office-prediction.ipynb` (17 KB) es un notebook de Kaggle que quedó en la carpeta: **no es una fuente de datos**, muévelo a `scripts\` o bórralo para no confundir a Hop al listar `raw\*`.
