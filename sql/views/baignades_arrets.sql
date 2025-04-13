DROP MATERIALIZED VIEW IF EXISTS public.baignades_nearest_stops CASCADE;

create materialized view if not exists baignades_nearest_stops as
   with ranked_stops as (
      select b.nom_du_site as nom,
             a.id as arret_id,
             a.name as arret_name,
             b.geog as geog_baignade,
             a.geog as geog_arret,
             st_distance(
                b.geog,
                a.geog
             ) as distance,
             row_number()
             over(partition by b.nom_du_site
                  order by b.geog <-> a.geog
             ) as rn
        from baignades b
        join arrets_transport a
      on st_dwithin(
         b.geog,
         a.geog,
         1000
      )
   )
   select nom,
          geog_baignade,
          arret_id,
          arret_name,
          geog_arret,
          distance
     from ranked_stops
    where rn = 1;