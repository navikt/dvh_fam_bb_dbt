{{ config(
    materialized='view'
) }}


SELECT
    CONNECT_BY_ROOT granted_role AS initial_access_grant,
    grantee                      AS final_user,
    LEVEL                        AS total_hops
FROM (
    SELECT 
        t1.grantee, 
        t1.granted_role, 
        t2.user_category
    FROM (select grantee, granted_role from {{ source('admin', 'dba_role_privs') }}
    union select grantee, 'SELECT ANY TABLE' as granted_role from {{ ref('test_tilgang_til_alt') }}
     ) t1
    LEFT JOIN {{ ref('test_brukere') }} t2 
    ON t1.grantee = t2.username
)
WHERE user_category = 'PERSONLIG_BRUKER'
CONNECT BY NOCYCLE PRIOR grantee = granted_role
