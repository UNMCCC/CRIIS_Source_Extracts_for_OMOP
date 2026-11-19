SELECT 
    'UNMMGDSS_P126_RAW.PERSON'           AS IDENTITY_CONTEXT,
    [person_id],
    [unmmgdss].p126_reporting.get_mrn_from_person_id([person_id]) AS MRN,
    CASE
        WHEN [sex_cd] = 358
            THEN 'F'
        WHEN [sex_cd] = 359
            THEN 'M'
        ELSE ''
    END                                  AS GENDER_CONCEPT_ID,
    YEAR([birth_dt_tm])                  AS YEAR_OF_BIRTH,
    MONTH([birth_dt_tm])                 AS MONTH_OF_BIRTH,
    DAY([birth_dt_tm])                   AS DAY_OF_BIRTH,
    FORMAT(TRY_CAST([birth_dt_tm] AS DATETIME), 'yyyy-MM-dd HH:mm:ss') AS BIRTH_DATETIME,
    ISNULL(FORMAT(TRY_CAST([deceased_dt_tm] AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') AS DECEASED_DATE,
    [race_cd]                            AS RACE_CONCEPT_ID,
    [ethnic_grp_cd]                      AS ETHNICITY_CONCEPT_ID,
    0                                    AS LOCATION_ID,
    0                                    AS PROVIDER_ID,
    0                                    AS CARE_SITE_ID,
    [person_id]                          AS PERSON_SOURCE_VALUE,
    CASE
        WHEN [sex_cd] = 358
            THEN 'F'
        WHEN [sex_cd] = 359
            THEN 'M'
        ELSE ''
    END                                  AS GENDER_SOURCE_VALUE,
    CASE
        WHEN [sex_cd] = 358
            THEN 'F'
        WHEN [sex_cd] = 359
            THEN 'M'
        ELSE ''
    END                                  AS GENDER_SOURCE_CONCEPT_ID,
    [race_cd]                            AS RACE_SOURCE_VALUE,
    [race_cd]                            AS RACE_SOURCE_CONCEPT_ID,
    [ethnic_grp_cd]                      AS ETHNICITY_SOURCE_VALUE,
    [ethnic_grp_cd]                      AS ETHNICITY_SOURCE_CONCEPT_ID,
    [updt_dt_tm]                         AS UPDT_DT_TM
FROM [unmmgdss].[p126_raw].[person] p
JOIN [unmmgdss].dss.pt_dim_v pdv ON [unmmgdss].p126_reporting.get_mrn_from_person_id(p.person_id) = pdv.pt_corrected_mrn