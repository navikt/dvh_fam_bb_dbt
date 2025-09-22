with nyeste_record as (
  select *
  from {{ref('fagsak_forskudd')}}
  where nr = 1
)

select * 
from nyeste_record