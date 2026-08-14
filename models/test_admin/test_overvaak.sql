with fagsak_forskudd as (
    select 
        'DAG' as typelinje, 
        'FORSKUDD' as stonadstype,
        trunc(lastet_dato) as lastet_dato, 
        count(distinct pk_bb_fagsak) as antfagsak, 
        min(vedtakstidspunkt) as tidspunkt_vedtak_min, 
        max(vedtakstidspunkt) as tidspunkt_vedtak_max,
        min(kafka_offset) as kafka_offset_min, 
        max(kafka_offset) as kafka_offset_max,
        count(distinct pk_bb_fagsak) - max(kafka_offset) + min(kafka_offset) - 1 as diff
    from {{ ref('fam_bb_fagsak') }}
    where lastet_dato > current_date - 30
    group by trunc(lastet_dato)
),

fagsak_bidrag as (
    select 
        'DAG' as typelinje, 
        'BIDRAG' as stonadstype,
        trunc(lastet_dato) as lastet_dato, 
        count(distinct pk_bb_fagsak) as antfagsak, 
        min(vedtakstidspunkt) as tidspunkt_vedtak_min, 
        max(vedtakstidspunkt) as tidspunkt_vedtak_max,
        min(kafka_offset) as kafka_offset_min, 
        max(kafka_offset) as kafka_offset_max,
        count(distinct pk_bb_fagsak) - max(kafka_offset) + min(kafka_offset) - 1 as diff
    from {{ ref('fam_bb_fagsak_ord') }}
    where lastet_dato > current_date - 30
    group by trunc(lastet_dato)
),

fagsak_saer as (
    select 
        'DAG' as typelinje, 
        'SÆRBIDRAG' as stonadstype,
        trunc(lastet_dato) as lastet_dato, 
        count(distinct fk_bb_meta_data) as antfagsak, 
        min(vedtaks_tid) as tidspunkt_vedtak_min, 
        max(vedtaks_tid) as tidspunkt_vedtak_max,
        min(kafka_offset) as kafka_offset_min, 
        max(kafka_offset) as kafka_offset_max,
        count(distinct fk_bb_meta_data) - max(kafka_offset) + min(kafka_offset) - 1 as diff
    from {{ ref('fam_bb_saerbidrag_fagsak') }}
    where trunc(lastet_dato) > current_date - 30
    group by trunc(lastet_dato)
),

final as (
    select * from fagsak_forskudd
    union all 
    select * from fagsak_bidrag
    union all 
    select * from fagsak_saer
)

select 
    typelinje,
    stonadstype,
    lastet_dato,
    antfagsak,
    tidspunkt_vedtak_min,
    tidspunkt_vedtak_max,
    kafka_offset_min,
    kafka_offset_max,
    diff
from final
order by stonadstype, lastet_dato
