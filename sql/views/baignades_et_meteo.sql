create or replace view baignades_meteo as
   select b.nom_du_site,
          b.categorie,
          b.baignade_surveillee,
          b.adresse,
          b.code_postal,
          b.ville,
          b.numero_de_telephone,
          b.geog as geog_baignade,
          m.timestamp as forecast_time,
          m.temperature as temperature,
          m.humidity as humidite,
          m.wind_speed as vitesse_vent,
          m.weather_description as description_meteo
     from baignades b
    cross join meteo_forecast m
    order by 
             m.timestamp, b.nom_du_site;