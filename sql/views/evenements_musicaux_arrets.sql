drop view if exists evenements_arrets;
create MATERIALIZED view if not exists evenements_arrets as
   with ranked_stops as (
      select e.titre as evenement,
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
          arret_id,
          arret_name,
          geog_evenement,
          geog_arret,
          distance
     from ranked_stops
    where rn = 1;