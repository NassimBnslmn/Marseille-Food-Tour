drop materialized view if exists restaurant_nearest_stops;

create materialized view if not exists restaurant_nearest_stops as
   with ranked_stops as (
      select r.nom as restaurant_name,
             a.id as arret_id,
             a.name as arret_name,
             r.geog as geog_restaurant,
             a.geog as geog_arret,
             r.google_note as note,
             r.review_count as review_count,
             st_distance(
                r.geog,
                a.geog
             ) as distance,
             row_number()
             over(partition by r.nom,
                               a.name
                  order by r.geog <-> a.geog
             ) as rn
        from restaurants r
        join arrets_transport a
      on st_dwithin(
         r.geog,
         a.geog,
         300
      )  -- only consider stops within 1km (adjust as needed)
   )
   select restaurant_name,
            note,
            review_count,
          geog_restaurant,
          arret_id,
          arret_name,
          geog_arret,
          distance,
          rn as stop_rank
     from ranked_stops
    where rn = 1  -- Ensures only the closest unique arret_name for each restaurant
    order by restaurant_name,
             stop_rank;