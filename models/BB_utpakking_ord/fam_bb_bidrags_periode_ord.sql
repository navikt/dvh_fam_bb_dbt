{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
    select * from {{ref ('bb_meldinger_til_aa_pakke_ut_ord')}} 
),

bb_fagsak as (
    select vedtaks_id, pk_bb_fagsak, kafka_offset
    from {{ref ('fam_bb_fagsak_ord')}}
),

pre_final as (
    select *
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id varchar2(255) path '$.vedtaksid',
                nested path '$.bidragPeriodeListe[*]'
                columns (
                    periode_fra                 varchar2(255) path '$.periodeFra'
                   ,periode_til                 varchar2(255) path '$.periodeTil'
                   ,belop                       number(18,2)  path '$.beløp'
                   ,resultat                    varchar2(255) path '$.resultat'
                   ,bidragsevne                 number(18,2)  path '$.bidragsevne'
                   ,underholdskostnad           number(18,2)  path '$.underholdskostnad'
                   ,samvaersfradrag             number(18,2)  path '$.samværsfradrag'
                   ,netto_barnetillegg_bp       number(18,2)  path '$.nettoBarnetilleggBP'
                   ,netto_barnetillegg_bm       number(18,2)  path '$.nettoBarnetilleggBM'
                   ,samvaersklasse              varchar2(255) path '$.samværsklasse'
                   ,bps_andel_underholdskostnad number(18,2)  path '$.bpsAndelUnderholdskostnad'
                   ,bpbor_med_andre_voksne      varchar2(255) path '$.bpborMedAndreVoksne'
                   ))
        ) j
    where periode_fra is not null
    --where json_value (melding, '$.forskuddPeriodeListe.size()' ) > 0
),

final as (
    select
        to_date(periode_fra,'yyyy-mm-dd') as periode_fra
       ,to_date(periode_til,'yyyy-mm-dd') as periode_til
       ,belop
       ,resultat
       ,bidragsevne
       ,underholdskostnad
       ,samvaersfradrag
       ,netto_barnetillegg_bp
       ,netto_barnetillegg_bm
       ,samvaersklasse
       ,bps_andel_underholdskostnad
       ,case
           when bpbor_med_andre_voksne = 'true' then '1'
           when bpbor_med_andre_voksne = 'false' then '0'
           else bpbor_med_andre_voksne  
        end bpbor_med_andre_voksne
       ,pre_final.kafka_offset
       ,bb_fagsak.pk_bb_fagsak as fk_bb_fagsak
    from pre_final
    join bb_fagsak
    on pre_final.kafka_offset = bb_fagsak.kafka_offset
    and pre_final.vedtaks_id = bb_fagsak.vedtaks_id
)

select dvh_fam_bb.dvh_fambb_kafka.nextval as pk_bb_bidrags_periode
    ,fk_bb_fagsak
    ,periode_fra
    ,periode_til
    ,belop
    ,resultat
    ,bidragsevne
    ,underholdskostnad
    ,samvaersfradrag
    ,netto_barnetillegg_bp
    ,netto_barnetillegg_bm
    ,samvaersklasse
    ,bps_andel_underholdskostnad
    ,bpbor_med_andre_voksne
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final