SELECT 'Incremental Data Syndication' AS config_source,
       'lookback_days'                AS config_type,
       '14'                           AS config_value
INTO   @omopDatabaseSchema.OMOP_INCR_CONFIG;
