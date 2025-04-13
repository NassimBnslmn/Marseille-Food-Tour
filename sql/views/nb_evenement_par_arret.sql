CREATE OR REPLACE VIEW nb_evenements_par_arret AS
SELECT arret_name, COUNT(*) AS nb_evenements
FROM evenements_arrets
GROUP BY arret_name
ORDER BY nb_evenements DESC;
