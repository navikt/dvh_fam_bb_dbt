with inntekt as (
    select *
    from {{ref ('stg_bb_inntekt')}}
),

bb_fagsak as (
    select vedtaks_id, pk_bb_fagsak, kafka_offset, fk_person1_kravhaver
    from {{ref ('int_bb_fagsak')}}
),


bb_forskudds_periode as (
    select periode_fra, periode_til, pk_bb_forskudds_periode, fk_bb_fagsak
    from {{ref ('int_bb_forskudds_periode')}}
),

final as (
    select
        type_inntekt
       ,belop
       ,fb.pk_bb_forskudds_periode as fk_bb_forskudds_periode
       ,fb.periode_fra
       ,fb.periode_til
       ,row_number() over (partition by bb_fagsak.vedtaks_id, type_inntekt order by inntekt.kafka_offset) as type_inntekt_nr
       ,inntekt.kafka_offset
       ,bb_fagsak.vedtaks_id
       ,bb_fagsak.fk_person1_kravhaver
    from inntekt 
    inner join bb_fagsak
        on inntekt.kafka_offset = bb_fagsak.kafka_offset
        and inntekt.vedtaks_id = bb_fagsak.vedtaks_id
    inner join bb_forskudds_periode fb
        on nvl(to_date(inntekt.periode_fra, 'yyyy-mm-dd'), DATE '2099-12-31') = nvl(fb.periode_fra, DATE '2099-12-31')
        and nvl(to_date(inntekt.periode_til, 'yyyy-mm-dd'), DATE '2099-12-31') = nvl(fb.periode_til, DATE '2099-12-31')
        and fb.fk_bb_fagsak = bb_fagsak.pk_bb_fagsak
)

select 
    f.*,
    standard_hash(vedtaks_id || '|' || type_inntekt || '|' || type_inntekt_nr || '|' || TO_CHAR(periode_fra, 'YYYY-MM-DD') || '|' || fk_person1_kravhaver,'MD5') pk_bb_inntekt
from final f