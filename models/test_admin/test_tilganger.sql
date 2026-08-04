select *
from {{ source('admin', 'dba_tab_privs') }}
where owner like 'DVH_FAM%'