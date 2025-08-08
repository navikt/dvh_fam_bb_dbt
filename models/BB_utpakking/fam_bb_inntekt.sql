{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
  select * from {{ref ('bb_meldinger_til_aa_pakke_ut')}}
),

bb_fagsak as (
    select vedtaks_id, pk_bb_fagsak, kafka_offset
    from {{ref ('fam_bb_fagsak')}}
),


bb_forskudds_periode as (
    select periode_fra, periode_til, pk_bb_forskudds_periode, fk_bb_fagsak
    from {{ref ('fam_bb_forskudds_periode')}}
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
),

final as (
    select
        type_inntekt
       ,belop
       ,bb_forskudds_periode.pk_bb_forskudds_periode as fk_bb_forskudds_periode
       ,bb_forskudds_periode.periode_fra
       ,bb_forskudds_periode.periode_til
       ,pre_final.kafka_offset
    from pre_final
    inner join bb_fagsak
    on pre_final.kafka_offset = bb_fagsak.kafka_offset
    and pre_final.vedtaks_id = bb_fagsak.vedtaks_id
    inner join bb_forskudds_periode
    on nvl(to_date(pre_final.periode_fra,'yyyy-mm-dd'),to_date('2099-12-31', 'yyyy-mm-dd')) = nvl(bb_forskudds_periode.periode_fra,to_date('2099-12-31', 'yyyy-mm-dd'))
    and nvl(to_date(pre_final.periode_til,'yyyy-mm-dd'),to_date('2099-12-31', 'yyyy-mm-dd')) = nvl(bb_forskudds_periode.periode_til,to_date('2099-12-31', 'yyyy-mm-dd'))
    and bb_forskudds_periode.fk_bb_fagsak = bb_fagsak.pk_bb_fagsak
)

select
    dvh_fam_bb.dvh_fambb_kafka.nextval as pk_bb_inntekt
   ,fk_bb_forskudds_periode
   ,case when type_inntekt = 'true' then '1'
         when type_inntekt = 'false' then '0'
         else type_inntekt
    end type_inntekt
   ,belop
   ,kafka_offset
   ,localtimestamp as lastet_dato
from final