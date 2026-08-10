with bb_meta_data as (
  select * from {{ref ('stg_bb_meta_data')}}
),

pre_final as (
    select *
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id varchar2(255) path '$.vedtaksid',
                nested path '$.forskuddPeriodeListe[*]'
                columns (
                    periode_fra varchar2(255) path '$.periodeFra',
                    periode_til varchar2(255) path '$.periodeTil',
                    nested path '$.inntektListe[*]'
                    columns (
                        type_inntekt varchar2(255) path '$.type',
                        belop        varchar2(255) path '$.beløp'
            )
          )
       )
    ) j
    --where json_value (melding, '$.forskuddPeriodeListe.inntektListe.size()' ) > 0
    where type_inntekt is not null
)

select 
    * 
from pre_final 