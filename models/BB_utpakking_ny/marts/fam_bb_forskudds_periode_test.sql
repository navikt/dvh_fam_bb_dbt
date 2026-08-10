
with forskudds_perioder as (
    select * from {{ref ('int_bb_forskudds_periode')}}
)

select 
    pk_bb_forskudds_periode
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
from forskudds_perioder

