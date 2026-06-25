SELECT has_identity
from {{ source('admin', 'dba_tables') }}
  where owner like 'DVH_FAM%'

