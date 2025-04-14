CREATE OR REPLACE VIEW baignades_best_conditions AS
SELECT nom_du_site, 
          b.categorie,
          b.baignade_surveillee,
          b.adresse,
          b.numero_de_telephone
 , forecast_time::date AS date, temperature, humidite, vitesse_vent, description_meteo
FROM baignades_meteo b
WHERE temperature >= 25 AND vitesse_vent <= 20
ORDER BY temperature DESC;
