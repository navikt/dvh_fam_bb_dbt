SELECT owner as skjema
,table_name
,has_identity
,num_rows
,sample_size
,last_analyzed
from {{ source('admin', 'dba_tables') }}
  where owner like 'DVH_FAM%'

