with bb_meta_data as (
  select 
    kafka_offset, melding 
  from {{ref ('stg_bb_meta_data')}}
),

final as (
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
    where type_inntekt is not null
)

select 
    kafka_offset
    ,vedtaks_id
    ,periode_fra
    ,periode_til
    ,type_inntekt
    ,belop
from final 