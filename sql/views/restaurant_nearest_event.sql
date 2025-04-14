DROP MATERIALIZED VIEW IF EXISTS restaurant_nearest_event CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.restaurant_nearest_event AS
WITH ranked_restaurants AS (
    SELECT 
        e.titre AS evenement,
        e.description AS evenement_description,
        e.adresse AS evenement_adresse,
        e.date_debut,
        e.date_fin,
        r.nom AS restaurant_name,
        r.google_note AS note,
        r.review_count AS review_count,
        r.description AS restaurant_description,
        r.specialites AS specialites,
        r.periode_ouverte AS periode_ouverte,
        r.geog AS geog_restaurant,
        ST_Distance(e.geog, r.geog) AS distance,
        e.geog AS geog_evenement,
        ROW_NUMBER() OVER (PARTITION BY e.titre ORDER BY e.geog <-> r.geog) AS rn
    FROM evenements_musicaux e
    JOIN restaurants r ON ST_DWithin(e.geog, r.geog, 400)
)
SELECT 
    evenement,
    evenement_description,
    evenement_adresse,
    date_debut,
    date_fin,
    restaurant_name,
    note,
    review_count,
    restaurant_description,
    specialites,
    periode_ouverte,
    geog_restaurant,
    distance,
    geog_evenement
FROM ranked_restaurants
WHERE rn <= 5;
