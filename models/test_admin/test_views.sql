select owner as skjema
,view_name
from {{ source('admin', 'dba_views') }}
where owner like 'DVH_FAM%'