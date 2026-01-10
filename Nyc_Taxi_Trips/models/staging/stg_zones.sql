{{ config(materialized='view') }}
/*cast into data types to avoid issues and renaming to standardize the naming of columns*/
select
  cast("locationid" as integer) as location_id, 
  cast("borough" as text)       as borough,
  cast("zone" as text)          as zone,
  cast("service_zone" as text)  as service_zone
from {{ source('raw', 'zones') }}