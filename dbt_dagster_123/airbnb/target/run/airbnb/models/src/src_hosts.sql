
  create or replace   view AIRBNB.DEV.src_hosts
  
  
  
  
  as (
    with raw_hosts as
(
    select * from  AIRBNB.RAW.RAW_HOSTS
)
select 
    id as host_id,
    name as host_name,
    is_superhost,
    created_at,
    updated_at
from raw_hosts
  );

