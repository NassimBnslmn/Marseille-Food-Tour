DROP MATERIALIZED VIEW IF EXISTS restaurant_nearest_stops;

CREATE MATERIALIZED VIEW IF NOT EXISTS restaurant_nearest_stops AS
WITH ranked_stops AS (
    SELECT 
        r.nom AS restaurant_name,
        r.description AS restaurant_description,
        r.specialites,
        r.periode_ouverte,
        r.google_note AS note,
        r.review_count AS review_count,
        r.geog AS geog_restaurant,
        a.id AS arret_id,
        a.name AS arret_name,
        a.geog AS geog_arret,
        ST_Distance(r.geog, a.geog) AS distance,
        ROW_NUMBER() OVER (
            PARTITION BY r.nom, a.name
            ORDER BY r.geog <-> a.geog
        ) AS rn
    FROM restaurants r
    JOIN arrets_transport a
        ON ST_DWithin(r.geog, a.geog, 400)  -- within 400m
)
SELECT 
    restaurant_name,
    restaurant_description,
    specialites,
    periode_ouverte,
    note,
    review_count,
    geog_restaurant,
    arret_id,
    arret_name,
    geog_arret,
    distance,
    rn AS stop_rank
FROM ranked_stops
WHERE rn <= 3
ORDER BY restaurant_name, stop_rank;
