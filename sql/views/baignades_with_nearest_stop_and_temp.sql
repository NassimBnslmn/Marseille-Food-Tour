CREATE OR REPLACE VIEW baignades_with_nearest_stop_and_temp AS
SELECT bns.nom,
    bns.arret_name,
    bns.categorie,
          bns.baignade_surveillee,
        bns.adresse,
        bns.numero_de_telephone,

    atb.avg_temp
FROM baignades_nearest_stops bns
JOIN avg_temp_per_baignade atb ON bns.nom = atb.nom_du_site;
