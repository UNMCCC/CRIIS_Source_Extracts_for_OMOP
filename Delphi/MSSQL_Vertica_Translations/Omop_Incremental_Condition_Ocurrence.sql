-- ============================================================
-- OMOP Incremental Condition Occurrence (MS SQL Server)
-- Translated from Vertica dialect
--
-- Key translation notes:
--   datediff('day', dt1, dt2) -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                 -> GETDATE()
--   ::int                     -> TRY_CAST(... AS INT)
--   ascii()                   -> ASCII()
--   GROUP BY positional       -> expanded to column names
-- ============================================================

INSERT INTO UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_CONDITION_OCCUR (
    identity_context,
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_display,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    condition_status_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value,
    updt_dt_tm
)
SELECT DISTINCT
    'UNMHSC_EDW_MILL_CDS.MD_F_CONDITION'                       AS identity_context,
    c.CONDITION_ID                                              AS condition_occurrence_id,
    c.PERSON_ID                                                 AS person_id,
    c.CONDITION_RAW_CODE                                        AS condition_concept_id,
    c.CONDITION_RAW_DISPLAY                                     AS CONDITION_DISPLAY,
    MIN(COALESCE(c.EFFECTIVE_DT_TM, e.registration_dt_tm, p.beg_effective_dt_tm))
                                                                AS condition_start_date,
    MIN(COALESCE(c.EFFECTIVE_DT_TM, e.registration_dt_tm, p.beg_effective_dt_tm))
                                                                AS condition_start_datetime,
    MAX(COALESCE(c.EFFECTIVE_DT_TM, e.registration_dt_tm, p.end_effective_dt_tm))
                                                                AS condition_end_date,
    MAX(COALESCE(c.EFFECTIVE_DT_TM, e.registration_dt_tm, p.end_effective_dt_tm))
                                                                AS condition_end_datetime,
    0                                                           AS condition_type_concept_id,
    COALESCE(c.STATUS_RAW_CODE, c.DIAGNOSIS_TYPE_RAW_CODE)     AS condition_status_concept_id,
    NULL                                                        AS stop_reason,
    c.RESPONSIBLE_PRSNL_ID                                      AS provider_id,
    c.ENCOUNTER_ID                                              AS visit_occurrence_id,
    vd.visit_detail_id,
    c.CONDITION_RAW_CODE                                        AS condition_source_value,
    c.CONDITION_RAW_CODE                                        AS condition_source_concept_id,
    COALESCE(c.STATUS_RAW_CODE, c.DIAGNOSIS_TYPE_RAW_CODE)     AS condition_status_source_value,
    MAX(c.UPDT_DT_TM)                                          AS updt_dt_tm
FROM (
    SELECT *
    FROM (
        SELECT
            condition_id,
            person_id,
            encounter_id,
            TRY_CAST(RESPONSIBLE_PRSNL_ID AS INT)   AS RESPONSIBLE_PRSNL_ID,
            CONDITION_RAW_CODE,
            CONDITION_RAW_DISPLAY,
            EFFECTIVE_DT_TM,
            STATUS_RAW_CODE,
            DIAGNOSIS_TYPE_RAW_CODE,
            ROW_NUMBER() OVER (
                PARTITION BY CONDITION_ID, PERSON_ID
                ORDER BY UPDT_DT_TM DESC
            )                                       AS condition_r,
            UPDT_DT_TM
        FROM UNMHSC_EDW_MILL_CDS.MD_F_CONDITION
    ) t
    WHERE condition_r = 1
) c
LEFT JOIN UNMHSC_EDW_OMOP.ua_omop_visit_detail vd
    ON  c.RESPONSIBLE_PRSNL_ID = vd.provider_Id
    AND c.ENCOUNTER_ID         = vd.VISIT_OCCURRENCE_ID
LEFT JOIN (
    SELECT
        PROBLEM_ID,
        MIN(BEG_EFFECTIVE_DT_TM)  AS BEG_EFFECTIVE_DT_TM,
        MAX(end_effective_dt_tm)  AS END_EFFECTIVE_DT_TM
    FROM UNMHSC_P126.PROBLEM
    GROUP BY PROBLEM_ID
) p ON c.CONDITION_ID = p.problem_id
LEFT JOIN (
    SELECT
        encounter_ID,
        MAX(registration_dt_tm) AS registration_dt_tm
    FROM UNMHSC_edw_mill_cds.md_f_encounter
    GROUP BY encounter_id
) e ON c.encounter_ID = e.encounter_id
WHERE COALESCE(c.EFFECTIVE_DT_TM, e.registration_dt_tm, p.beg_effective_dt_tm) IS NOT NULL
  AND ASCII(c.CONDITION_RAW_CODE) != 32
  AND DATEDIFF(DAY, c.UPDT_DT_TM, GETDATE())
      <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
GROUP BY
    c.CONDITION_ID,
    c.PERSON_ID,
    c.ENCOUNTER_ID,
    c.CONDITION_RAW_CODE,
    c.CONDITION_RAW_DISPLAY,
    c.condition_type_concept_id,    -- 0 constant
    COALESCE(c.STATUS_RAW_CODE, c.DIAGNOSIS_TYPE_RAW_CODE),
    c.RESPONSIBLE_PRSNL_ID,
    vd.visit_detail_id;
