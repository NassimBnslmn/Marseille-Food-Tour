DROP MATERIALIZED VIEW IF EXISTS mini_itineraire_event_good_restaurant_arret CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mini_itineraire_event_good_restaurant_arret AS
WITH nearest_restaurants AS (
    SELECT e.titre AS evenement,
           r.nom AS restaurant_name,
           r.google_note AS note,
           r.review_count,
           r.geog AS geog_restaurant,
           ST_Distance(e.geog, r.geog) AS distance_event_restaurant,
           e.geog AS geog_evenement,
           ROW_NUMBER() OVER (PARTITION BY e.titre ORDER BY e.geog <-> r.geog) AS rn_restaurant
    FROM evenements_musicaux e
    JOIN restaurants r
      ON ST_DWithin(e.geog, r.geog, 1000)
     AND r.google_note >= 4
     AND r.review_count > 3
),
event_to_restaurant AS (
    SELECT *
    FROM nearest_restaurants
    WHERE rn_restaurant = 1
),
nearest_stops AS (
    SELECT er.evenement,
           er.restaurant_name,
           er.note,
           er.review_count,
           er.geog_evenement,
           er.geog_restaurant,
           er.distance_event_restaurant,
           a.id AS arret_id,
           a.name AS arret_name,
           a.geog AS geog_arret,
           ST_Distance(er.geog_restaurant, a.geog) AS distance_restaurant_arret,
           ROW_NUMBER() OVER (
               PARTITION BY er.evenement
               ORDER BY er.geog_restaurant <-> a.geog
           ) AS rn_stop
    FROM event_to_restaurant er
    JOIN arrets_transport a ON ST_DWithin(er.geog_restaurant, a.geog, 1000)
)
SELECT evenement,
       restaurant_name,
       note,
       review_count,
       arret_name,
       arret_id,
       distance_event_restaurant,
       distance_restaurant_arret,
       geog_evenement,
       geog_restaurant,
       geog_arret
FROM nearest_stops
WHERE rn_stop = 1
ORDER BY distance_event_restaurant, distance_restaurant_arret;