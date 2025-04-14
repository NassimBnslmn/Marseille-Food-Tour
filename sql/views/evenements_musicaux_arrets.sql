drop materialized view if exists evenements_arrets;
create MATERIALIZED view if not exists evenements_arrets as
   with ranked_stops as (
      select e.titre as evenement,
                e.description as evenement_description,
                e.nom_lieu as evenement_lieu,
                e.date_debut as debut_evenement,
                e.date_fin as fin_evenement,
             a.id as arret_id,
             a.name as arret_name,
             e.geog as geog_evenement,
             a.geog as geog_arret,
             st_distance(
                e.geog,
                a.geog
             ) as distance,
             row_number()
             over(partition by e.titre
                  order by e.geog <-> a.geog
             ) as rn
        from evenements_musicaux e
        join arrets_transport a
      on st_dwithin(
         e.geog,
         a.geog,
         400
      )
   )
   select evenement,
            evenement_description,
            evenement_lieu,
            debut_evenement,
            fin_evenement,
          arret_id,
          arret_name,
          geog_evenement,
          geog_arret,
          distance
     from ranked_stops
    where rn <= 3;