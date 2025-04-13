CREATE OR REPLACE VIEW nb_restaurants_par_arret AS
SELECT arret_name, COUNT(*) AS nb_restaurants
FROM restaurant_nearest_stops
GROUP BY arret_name
ORDER BY nb_restaurants DESC;
