CREATE EXTENSION IF NOT EXISTS postgis;

ALTER TABLE restaurants
ADD COLUMN geom GEOMETRY(Point, 4326);

UPDATE restaurants
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL AND latitude IS NOT NULL;




-- Views for nearest stops to restaurants
-- This view will find the nearest transport stops to each restaurant


DROP VIEW IF EXISTS restaurant_nearest_stops;

CREATE MATERIALIZED VIEW restaurant_nearest_stops AS
WITH ranked_stops AS (
    SELECT 
        r.nom AS restaurant_name,
        a.id AS arret_id,
        a.name AS arret_name,
        ST_Distance(r.geom, a.geom) AS distance,
        ROW_NUMBER() OVER (PARTITION BY r.nom, a.name ORDER BY r.geom <-> a.geom) AS rn
    FROM restaurants r
    JOIN arrets_transport a 
      ON ST_DWithin(r.geom, a.geom, 1000)  -- only consider stops within 1km (adjust as needed)
)
SELECT
    restaurant_name,
    arret_id,
    arret_name,
    distance,
    rn AS stop_rank
FROM ranked_stops
WHERE rn = 1  -- Ensures only the closest unique arret_name for each restaurant
ORDER BY restaurant_name, stop_rank;



-- Selecting the top 3 nearest stops for each restaurant using the materialized view


WITH ranked_stops AS (
    SELECT 
        restaurant_name,
        arret_id,
        arret_name,
        distance,
        ROW_NUMBER() OVER (PARTITION BY restaurant_name ORDER BY distance) AS rn
    FROM restaurant_nearest_stops
)
SELECT
    restaurant_name,
    arret_id,
    arret_name,
    distance,
    rn AS stop_rank
FROM ranked_stops
WHERE rn <= 3  -- Limits each restaurant to a maximum of 3 stops
ORDER BY restaurant_name, stop_rank;




-- 🧭 Add geography column
ALTER TABLE baignades
ADD COLUMN IF NOT EXISTS geog geography(Point, 4326);

-- 📍 Populate geography column from lon/lat
UPDATE baignades
SET geog = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
WHERE longitude IS NOT NULL AND latitude IS NOT NULL;





CREATE OR REPLACE VIEW baignades_with_forecast AS
SELECT
    b.nom_du_site,
    b.categorie,
    b.baignade_surveillee,
    b.adresse,
    b.code_postal,
    b.ville,
    b.numero_de_telephone,
    b.longitude,
    b.latitude,
    m.timestamp AS forecast_time,
    m.temperature,
    m.humidity,
    m.wind_speed,
    m.weather_description
FROM
    baignades b
CROSS JOIN
    meteo_forecast m
ORDER BY
    b.nom_du_site, m.timestamp;

SELECT *
FROM baignades_with_forecast
WHERE forecast_time::date = CURRENT_DATE
  AND ville = 'Marseille'
  AND baignade_surveillee = true;
