{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
    select *
    from {{ref ('bb_meldinger_til_aa_pakke_ut_ord')}}
),

bb_fagsak as (
    select vedtaks_id, pk_bb_fagsak, kafka_offset
    from {{ref ('fam_bb_fagsak_ord')}}
),


bb_bidrags_periode as (
    select periode_fra, periode_til, pk_bb_bidrags_periode, fk_bb_fagsak
    from {{ref ('fam_bb_bidrags_periode_ord')}}
),

bp as (
    select
        kafka_offset,
        vedtaks_id,
        periode_fra,
        periode_til,
        type_inntekt,
        belop,
        'P' as flagg
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id varchar2(255) path '$.vedtaksid',
                nested path '$.bidragPeriodeListe[*]'
                columns (
                    periode_fra varchar2(255) path '$.periodeFra',
                    periode_til varchar2(255) path '$.periodeTil',
                    nested path '$.bpinntektListe[*]'
                    columns (
                        type_inntekt varchar2(255) path '$.type',
                        belop        number(18,2)  path '$.beløp'
                            )
                        )
                    )
         )j
    where type_inntekt is not null
),

bm as (
    select
        kafka_offset,
        vedtaks_id,
        periode_fra,
        periode_til,
        type_inntekt,
        belop,
        'M' as flagg
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id varchar2(255) path '$.vedtaksid',
                nested path '$.bidragPeriodeListe[*]'
                columns (
                    periode_fra varchar2(255) path '$.periodeFra',
                    periode_til varchar2(255) path '$.periodeTil',
                    nested path '$.bminntektListe[*]'
                    columns (
                        type_inntekt varchar2(255) path '$.type',
                        belop number(18,2) path '$.beløp'
                            )
                        )
                    )
        )j
    where type_inntekt is not null
),

pre_final as (
    select * from bp
    union all
    select * from bm
),

final as (
    select
        pf.type_inntekt,
        pf.belop,
        pf.flagg,
        bb.pk_bb_bidrags_periode as fk_bb_bidrags_periode,
        bb.periode_fra,
        bb.periode_til,
        pf.kafka_offset
    from pre_final pf
    
    join bb_fagsak bf
    on pf.kafka_offset = bf.kafka_offset
    and pf.vedtaks_id = bf.vedtaks_id
    
    join bb_bidrags_periode bb
    on nvl(to_date(pf.periode_fra, 'yyyy-mm-dd'), to_date('2099-12-31', 'yyyy-mm-dd')) = nvl(bb.periode_fra, to_date('2099-12-31', 'yyyy-mm-dd'))
    and nvl(to_date(pf.periode_til, 'yyyy-mm-dd'), to_date('2099-12-31', 'yyyy-mm-dd')) = nvl(bb.periode_til, to_date('2099-12-31', 'yyyy-mm-dd'))
    and bb.fk_bb_fagsak = bf.pk_bb_fagsak
)

select 
    dvh_fam_bb.dvh_fambb_kafka.nextval as pk_bb_inntekt,
    fk_bb_bidrags_periode,
    type_inntekt,
    belop,
    flagg,
    kafka_offset,
    localtimestamp as lastet_dato
from final