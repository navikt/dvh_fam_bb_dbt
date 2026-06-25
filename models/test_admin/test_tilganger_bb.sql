select *
from {{ source('admin', 'user_tab_privs') }}
where owner like 'DVH_FAM%'