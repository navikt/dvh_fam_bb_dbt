-- DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.pk_bb_saerbidrag_fagsak IS '#NAVN pk_bb_saerbidrag_fagsak #INNHOLD Unik nøkkel for tabellen.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.kafka_offset IS '#NAVN kafka_offset #INNHOLD Kafka-kø sekvensnummer.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.fk_bb_meta_data IS '#NAVN fk_bb_meta_data #INNHOLD Syntetisk fremmednøkkel mot kildetabellen.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.vedtaks_id IS '#NAVN vedtaks_id #INNHOLD Vedtakets ID.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.vedtaks_tidspunkt IS '#NAVN vedtaks_tidspunkt #INNHOLD Vedtakets tidspunkt.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.bidragstype IS '#NAVN bidragstype #INNHOLD Vedtakets type, for eksempel fastsettelse, endring eller klage.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.kategori IS '#NAVN kategori #INNHOLD Vedtakets kategori, for eksempel tannregulering.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.saksnr IS '#NAVN saksnr #INNHOLD Saksnummeret vedtaket er en del av.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.fk_person1_skyldner IS '#NAVN fk_person1_skyldner #INNHOLD Syntetisk identifikator for skyldner.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.fk_person1_kravhaver IS '#NAVN fk_person1_kravhaver #INNHOLD Syntetisk identifikator for kravhaver.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.fk_person1_mottaker IS '#NAVN fk_person1_mottaker #INNHOLD Syntetisk identifikator for mottaker.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.belop IS '#NAVN belop #INNHOLD Beløp.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.valuta_kode IS '#NAVN valuta_kode #INNHOLD Valutakoden tilknyttet beløpet.';
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.resultat IS '#NAVN resultat #INNHOLD Vedtakets resultat, for eksempel innvilget eller avslag.'
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.innkreving_flagg IS '#NAVN innkreving_flagg #INNHOLD Binær verdi som viser om vedtaket er registrert for innkreving via Skatt.'
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.omgjor_vedtaks_id IS '#NAVN omgjor_vedtaks_id #INNHOLD Viser til hvilken vedtaks id et vedtak skal omgjøre, dersom det skjer endringer på et vedtak.'
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.historisk_flagg IS '#NAVN historisk_flagg #INNHOLD Binær verdi som viser vedtakets systemtilknytning. Hvis 1 er det snakk om eldre fagsystem.'
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.krav_belop IS '#NAVN krav_belop #INNHOLD Kravbeløp.'
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.godkjent_belop IS '#NAVN godkjent_belop #INNHOLD Godkjent beløp.'
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.betalt_belop IS '#NAVN betalt_belop #INNHOLD Betalt beløp.'
COMMENT ON COLUMN DVH_FAM_BB.FAM_BB_SAERBIDRAG_FAGSAK.lastet_dato IS '#NAVN lastet_dato #INNHOLD Data lastet inn i tabellen i datavarehuset.'