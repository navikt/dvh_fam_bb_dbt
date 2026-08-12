with bb_forskudds_perioder as (
    select * from {{ref ('stg_bb_forskudds_periode')}}
),

fagsak as (
    select vedtaks_id, pk_bb_fagsak, kafka_offset, fk_person1_kravhaver, SAKSNR
    from {{ref ('int_bb_fagsak')}}
),

final as (
    select
        to_date(periode_fra,'yyyy-mm-dd') as periode_fra
       ,to_date(periode_til,'yyyy-mm-dd') as periode_til
       ,belop
       ,fagsak.vedtaks_id
       ,fagsak.saksnr
       ,fagsak.fk_person1_kravhaver
       ,resultat
       ,barnets_alders_gruppe
       ,antall_barn_i_egen_husstand
       ,sivilstand
       ,case
           when barn_bor_med_bm = 'true' then '1'
           when barn_bor_med_bm = 'false' then '0'
           else barn_bor_med_bm  
        end barn_bor_med_bm
       ,bb_forskudds_perioder.kafka_offset
       ,fagsak.pk_bb_fagsak as fk_bb_fagsak
    from bb_forskudds_perioder
    join fagsak
    on bb_forskudds_perioder.kafka_offset = fagsak.kafka_offset
    and bb_forskudds_perioder.vedtaks_id = fagsak.vedtaks_id
)

select 
    STANDARD_HASH(vedtaks_id || '|' || fk_person1_kravhaver || '|' || TO_CHAR(periode_fra, 'YYYY-MM-DD'),'MD5') AS pk_bb_forskudds_periode
    ,periode_fra
    ,periode_til
    ,belop
    ,resultat
    ,barnets_alders_gruppe
    ,antall_barn_i_egen_husstand
    ,sivilstand
    ,barn_bor_med_bm
    ,kafka_offset
    ,fk_bb_fagsak
from final
