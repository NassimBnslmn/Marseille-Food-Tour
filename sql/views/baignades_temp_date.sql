CREATE OR REPLACE VIEW avg_temp_per_baignade AS
SELECT
  nom_du_site,
  forecast_time::date AS date,
  AVG(temperature) AS avg_temp
FROM baignades_meteo
GROUP BY nom_du_site, forecast_time::date
ORDER BY date, avg_temp DESC;
