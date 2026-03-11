-- ============================================================
-- OMOP Incremental Drug Exposure (MS SQL Server)
-- Translated from Vertica dialect
--
-- Sources: MD_F_MEDICATION (Pharmacy) + MD_F_IMMUNIZATION
--
-- Key translation notes:
--   datediff('day', dt1, dt2)       -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                       -> GETDATE()
--   ::VARCHAR / ::INT / ::FLOAT     -> TRY_CAST(... AS ...)
--   (IMMUNIZATION_ID||encounter_id)::VARCHAR -> CONCAT() / CAST()
--   CHR(13), CHR(10)                -> CHAR(13), CHAR(10)
--   || (concat)                     -> CONCAT() or +
--   ORDER BY in CTEs removed        (invalid without TOP)
--   GROUP BY positional 1-25        -> expanded to column names
--   UNION with WHERE after          -> wrap in outer SELECT
-- ============================================================

;WITH Pharmacy AS (
    SELECT DISTINCT
        TRY_CAST(m.MEDICATION_ID AS VARCHAR(MAX))           AS drug_exposure_id,
        'UNMHSC_EDW_MILL_CDS.MD_F_MEDICATION'              AS identity_context,
        m.PERSON_ID                                         AS person_id,
        TRY_CAST(m.drug_raw_code AS VARCHAR(MAX))           AS drug_concept_id,
        m.START_DT_TM                                       AS drug_exposure_start_date,
        m.START_DT_TM                                       AS drug_exposure_start_datetime,
        m.STOP_DT_TM                                        AS drug_exposure_end_date,
        m.STOP_DT_TM                                        AS drug_exposure_end_datetime,
        m.STOP_DT_TM                                        AS verbatim_end_date,
        0                                                   AS drug_type_concept_id,
        TRY_CAST(m.STOP_TYPE_RAW_CODE AS VARCHAR(MAX))      AS stop_reason,
        0                                                   AS refills,
        TRY_CAST(m.DOSE_QUANTITY AS FLOAT)                  AS quantity,
        0                                                   AS days_supply,
        REPLACE(REPLACE(m.PATIENT_INSTRUCTIONS, CHAR(13), ''), CHAR(10), '')
                                                            AS sig,
        TRY_CAST(m.ROUTE_raw_code AS VARCHAR(MAX))          AS route_concept_id,
        NULL                                                AS lot_number,
        TRY_CAST(m.ORDER_PROVIDER_ID AS INT)                AS provider_id,
        TRY_CAST(vd.VISIT_OCCURRENCE_ID AS INT)             AS VISIT_OCCURRENCE_ID,
        vd.VISIT_DETAIL_ID,
        TRY_CAST(m.drug_raw_code AS VARCHAR(MAX))           AS drug_source_value,
        TRY_CAST(m.drug_raw_code AS VARCHAR(MAX))           AS drug_source_concept_id,
        TRY_CAST(m.ROUTE_raw_code AS VARCHAR(MAX))          AS route_source_value,
        TRY_CAST(m.DOSE_UNIT_raw_CODE AS VARCHAR(MAX))      AS dose_unit_source_value,
        m.UPDT_DT_TM
    FROM UNMHSC_EDW_MILL_CDS.MD_F_MEDICATION m
    LEFT JOIN UNMHSC_EDW_OMOP.UA_OMOP_VISIT_DETAIL vd
        ON  m.ENCOUNTER_ID       = vd.VISIT_OCCURRENCE_ID
        AND m.ORDER_PROVIDER_ID  = vd.PROVIDER_ID
    WHERE DATEDIFF(DAY, m.UPDT_DT_TM, GETDATE())
          <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
    GROUP BY
        TRY_CAST(m.MEDICATION_ID AS VARCHAR(MAX)),
        m.PERSON_ID,
        TRY_CAST(m.drug_raw_code AS VARCHAR(MAX)),
        m.START_DT_TM,
        m.STOP_DT_TM,
        TRY_CAST(m.STOP_TYPE_RAW_CODE AS VARCHAR(MAX)),
        TRY_CAST(m.DOSE_QUANTITY AS FLOAT),
        REPLACE(REPLACE(m.PATIENT_INSTRUCTIONS, CHAR(13), ''), CHAR(10), ''),
        TRY_CAST(m.ROUTE_raw_code AS VARCHAR(MAX)),
        TRY_CAST(m.ORDER_PROVIDER_ID AS INT),
        TRY_CAST(vd.VISIT_OCCURRENCE_ID AS INT),
        vd.VISIT_DETAIL_ID,
        TRY_CAST(m.DOSE_UNIT_raw_CODE AS VARCHAR(MAX)),
        m.UPDT_DT_TM
),

Immunization AS (
    SELECT DISTINCT
        TRY_CAST(
            CAST(i.IMMUNIZATION_ID AS NVARCHAR(50))
            + CAST(i.encounter_id AS NVARCHAR(50))
        AS VARCHAR(MAX))                                    AS drug_exposure_id,
        'UNMHSC_EDW_MILL_CDS.MD_F_IMMUNIZATION'            AS identity_context,
        i.PERSON_ID,
        TRY_CAST(i.Immunization_raw_code AS VARCHAR(MAX))   AS drug_concept_id,
        i.IMMUNIZATION_DT_TM                                AS drug_exposure_start_date,
        i.IMMUNIZATION_DT_TM                                AS drug_exposure_start_datetime,
        i.EXPIRE_DT_TM                                      AS drug_exposure_end_date,
        i.EXPIRE_DT_TM                                      AS drug_exposure_end_datetime,
        i.EXPIRE_DT_TM                                      AS verbatim_end_date,
        0                                                   AS drug_type_concept_id,
        NULL                                                AS stop_reason,
        0                                                   AS refills,
        TRY_CAST(i.DOSE_QUANTITY AS FLOAT)                  AS quantity,
        0                                                   AS DAYS_SUPPLY,
        NULL                                                AS sig,
        TRY_CAST(i.ADMIN_ROUTE_raw_CODE AS VARCHAR(MAX))    AS route_concept_id,
        TRY_CAST(i.lot AS VARCHAR(MAX))                     AS lot_number,
        TRY_CAST(i.ASSOCIATED_PROVIDER_ID AS INT)           AS provider_id,
        TRY_CAST(i.ENCOUNTER_ID AS INT)                     AS VISIT_OCCURRENCE_ID,
        vd.VISIT_DETAIL_ID,
        TRY_CAST(i.Immunization_raw_code AS VARCHAR(MAX))   AS drug_source_value,
        TRY_CAST(i.Immunization_raw_code AS VARCHAR(MAX))   AS drug_source_concept_id,
        TRY_CAST(i.ADMIN_ROUTE_raw_CODE AS VARCHAR(MAX))    AS route_source_value,
        TRY_CAST(i.DOSE_UNIT_raw_CODE AS VARCHAR(MAX))      AS dose_unit_source_value,
        i.UPDT_DT_TM
    FROM UNMHSC_EDW_MILL_CDS.MD_F_IMMUNIZATION i
    LEFT JOIN UNMHSC_EDW_OMOP.UA_OMOP_VISIT_DETAIL vd
        ON  i.ENCOUNTER_ID   = vd.VISIT_OCCURRENCE_ID
        AND i.UPDT_PRSNL_ID  = vd.PROVIDER_ID
    WHERE DATEDIFF(DAY, i.UPDT_DT_TM, GETDATE())
          <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
    GROUP BY
        TRY_CAST(
            CAST(i.IMMUNIZATION_ID AS NVARCHAR(50))
            + CAST(i.encounter_id AS NVARCHAR(50))
        AS VARCHAR(MAX)),
        i.PERSON_ID,
        TRY_CAST(i.Immunization_raw_code AS VARCHAR(MAX)),
        i.IMMUNIZATION_DT_TM,
        i.EXPIRE_DT_TM,
        TRY_CAST(i.DOSE_QUANTITY AS FLOAT),
        TRY_CAST(i.ADMIN_ROUTE_raw_CODE AS VARCHAR(MAX)),
        TRY_CAST(i.lot AS VARCHAR(MAX)),
        TRY_CAST(i.ASSOCIATED_PROVIDER_ID AS INT),
        TRY_CAST(i.ENCOUNTER_ID AS INT),
        vd.VISIT_DETAIL_ID,
        TRY_CAST(i.DOSE_UNIT_raw_CODE AS VARCHAR(MAX)),
        i.UPDT_DT_TM
)

SELECT DISTINCT *
FROM (
    SELECT DISTINCT * FROM Pharmacy
    UNION
    SELECT DISTINCT * FROM Immunization
) combined
WHERE drug_concept_id IS NOT NULL;
