CREATE OR REPLACE VIEW baignades_best_conditions AS
SELECT nom_du_site, forecast_time::date AS date, temperature, humidite, vitesse_vent
FROM baignades_meteo
WHERE temperature >= 25 AND humidite <= 60 AND vitesse_vent <= 20
ORDER BY temperature DESC;
