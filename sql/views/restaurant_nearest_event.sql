DROP MATERIALIZED VIEW IF EXISTS restaurant_nearest_event CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS public.restaurant_nearest_event AS
WITH ranked_restaurants AS (
    SELECT e.titre AS evenement,
           r.nom AS restaurant_name,
           r.google_note AS note,
           r.review_count AS review_count,
           r.geog AS geog_restaurant,
           ST_Distance(e.geog, r.geog) AS distance,
           e.geog AS geog_evenement,
           ROW_NUMBER() OVER (PARTITION BY e.titre ORDER BY e.geog <-> r.geog) AS rn
    FROM evenements_musicaux e
    JOIN restaurants r ON ST_DWithin(e.geog, r.geog, 1000)
)
SELECT evenement,
       restaurant_name,
       note,
       review_count,
               geog_restaurant,
       distance,
       geog_evenement
FROM ranked_restaurants
WHERE rn = 1;
