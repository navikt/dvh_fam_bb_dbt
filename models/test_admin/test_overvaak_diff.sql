with overvaak as (
    select *
    from {{ ref('test_overvaak') }}
),

final as (
     SELECT * 
FROM ( 
    SELECT
    lastet_dato,
   stonadstype,
   diff
    FROM overvaak
) 
PIVOT ( 
    SUM(diff)  
    FOR stonadstype IN ( 
        'FORSKUDD' AS FORSKUDD,
        'BIDRAG' AS BIDRAG,
        'SÆRBIDRAG' as saerbidrag
    ) 
) piv
)

select *
from final
order by lastet_dato desc