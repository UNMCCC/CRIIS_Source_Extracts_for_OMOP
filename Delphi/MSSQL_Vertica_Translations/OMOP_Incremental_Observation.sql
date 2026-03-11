-- ============================================================
-- OMOP Incremental Observation (MS SQL Server)
-- Translated from Vertica dialect
-- Mnemonic: OMOP_INCREMENTAL_OBSERVATION
-- Schema:   UNMHSC_EDW_OMOP
--
-- Key translation notes:
--   ILIKE                     -> LIKE  (case-insensitive collation assumed;
--                                       use LOWER(x) LIKE LOWER(y) if collation is CS)
--   datediff('day', dt1, dt2) -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                 -> GETDATE()
--   ::! INT                   -> TRY_CAST(... AS INT)   (soft cast, NULL on failure)
--   ::! Varchar               -> TRY_CAST(... AS VARCHAR(MAX))
--   CHR(13), CHR(10)          -> CHAR(13), CHAR(10)
--   || (concat)               -> CONCAT() or +
--   abs(mod(hash(...),10))    -> ABS(CHECKSUM(...) % 10)
--   ORDER BY in CTEs removed  (invalid without TOP in SQL Server)
-- ============================================================

;WITH form AS (
    SELECT DISTINCT
        f.DCP_FORMS_REF_ID,
        f.DCP_FORM_INSTANCE_ID,
        f.description       AS form_description,
        f.definition        AS form_definition,
        s.dcp_section_ref_id,
        i.DCP_INPUT_REF_ID,
        s.description       AS section_description,
        dta.description     AS dta_description,
        dta.mnemonic        AS dta_mnemonic,
        DTA.TASK_ASSAY_CD,
        v5c.event_cd_disp,
        v5c.event_set_name,
        v5c.event_cd,
        d.updt_dt_tm
    FROM UNMHSC_P126.DCP_FORMS_DEF D
    JOIN UNMHSC_P126.DCP_FORMS_REF F
        ON  F.DCP_FORM_INSTANCE_ID = D.DCP_FORM_INSTANCE_ID
        AND D.DCP_FORMS_REF_ID IN ('620801790','1901378383','473445','624468464')
        AND F.ACTIVE_IND = 1
    JOIN UNMHSC_P126.DCP_SECTION_REF S
        ON  S.DCP_SECTION_REF_ID = D.DCP_SECTION_REF_ID
        AND S.ACTIVE_IND = 1
    JOIN UNMHSC_P126.DCP_INPUT_REF I
        ON  I.DCP_SECTION_REF_ID = S.DCP_SECTION_REF_ID
        AND I.ACTIVE_IND = 1
    JOIN UNMHSC_P126.NAME_VALUE_PREFS PRF
        ON  I.DCP_INPUT_REF_ID = PRF.PARENT_ENTITY_ID
        AND PRF.ACTIVE_IND = 1
    JOIN UNMHSC_P126.DISCRETE_TASK_ASSAY DTA
        ON  PRF.MERGE_ID  = DTA.TASK_ASSAY_CD
        AND DTA.ACTIVE_IND = 1
    LEFT JOIN UNMHSC_P126.V500_EVENT_CODE V5C
        ON DTA.EVENT_CD = V5C.EVENT_CD
    WHERE D.ACTIVE_IND = 1
      -- ILIKE translated to LIKE (case-insensitive collation assumed)
      AND NOT (
            f.description   LIKE 'ZZ%'
         OR f.definition    LIKE 'ZZ%'
         OR S.description   LIKE 'ZZ%'
      )
),

death_description AS (
    SELECT
        a.CLINICAL_EVENT_ID,
        a.ENCNTR_ID,
        a.PERSON_ID,
        a.EVENT_ID,
        a.event_cd,
        ce.event_tag,
        a.event_end_dt_tm,
        a.result_val,
        a.TASK_ASSAY_CD,
        a.verified_prsnl_id,
        a.UPDT_DT_TM
    FROM UNMHSC_P126.CLINICAL_EVENT ce
    JOIN (
        SELECT
            CLINICAL_EVENT_ID,
            ENCNTR_ID,
            PERSON_ID,
            EVENT_ID,
            PARENT_EVENT_ID,
            event_cd,
            EVENT_TAG,
            event_end_dt_tm,
            result_val,
            TASK_ASSAY_CD,
            verified_prsnl_id,
            UPDT_DT_TM
        FROM UNMHSC_P126.clinical_event
        WHERE EVENT_cd IN ('782592','782596','782589')
          -- 782592=age of death, 782596=cause of death, 782589=status
          AND DATEDIFF(DAY, UPDT_DT_TM, GETDATE())
              <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
    ) a ON  ce.EVENT_ID  = a.parent_event_id
         AND ce.PERSON_ID = a.person_id
    WHERE ce.event_tag NOT IN ('In Progress','In Error')
)

INSERT INTO UNMHSC_EDW_OMOP.OMOP_INCR_OBSERVATION (
    identity_context,
    observation_id,
    person_id,
    event_cd,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    form_description,
    section_description,
    description,
    secondary_description,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_id,
    value_source_value,
    observation_event_id,
    obs_event_field_concept_id,
    updt_dt_tm
)
SELECT DISTINCT
    'UNMHSC_P126.DCP_FORMS_DEF'                         AS Identity_Context,
    CAST(ce.CLINICAL_EVENT_ID AS NVARCHAR(MAX))
    + CAST(pf.DCP_FORM_INSTANCE_ID AS NVARCHAR(MAX))
    + CAST(pf.DCP_SECTION_REF_ID AS NVARCHAR(MAX))
    + CAST(pf.TASK_ASSAY_CD AS NVARCHAR(MAX))           AS observation_id,
    ce.person_id,
    pf.event_cd,
    ce.event_end_dt_tm                                  AS observation_date,
    ce.EVENT_END_DT_TM                                  AS observation_datetime,
    pf.DCP_FORMS_REF_ID                                 AS Observation_type_concept_id,
    pf.form_description,
    pf.section_description,
    pf.dta_description                                  AS description,
    ds.event_tag                                        AS secondary_description,
    TRY_CAST(ce.result_val AS INT)                      AS value_as_number,
    REPLACE(REPLACE(TRY_CAST(ce.result_val AS VARCHAR(MAX)), CHAR(13), ''), CHAR(10), '')
                                                        AS value_as_string,
    0                                                   AS VALUE_AS_CONCEPT_ID,
    0                                                   AS qualifier_concept_id,
    0                                                   AS UNIT_CONCEPT_ID,
    ce.verified_prsnl_id                                AS PROVIDER_ID,
    ce.encntr_id                                        AS VISIT_OCCURRENCE_ID,
    NULL                                                AS VISIT_DETAIL_ID,
    NULL                                                AS Observation_source_value,
    NULL                                                AS Observation_source_concept_id,
    NULL                                                AS UNIT_SOURCE_VALUE,
    NULL                                                AS qualifier_source_id,
    NULL                                                AS VALUE_SOURCE_VALUE,
    NULL                                                AS observation_event_id,
    NULL                                                AS obs_event_field_concept_id,
    ce.UPDT_DT_TM
FROM form pf
JOIN UNMHSC_P126.clinical_event ce
    ON pf.task_assay_cd = ce.task_assay_cd
LEFT JOIN death_description ds
    ON ds.task_assay_cd = pf.task_assay_cd
LEFT JOIN UNMHSC_P126.prsnl prsnl
    ON  ce.verified_prsnl_id = prsnl.person_id
    AND prsnl.active_ind     = 1
LEFT JOIN UNMHSC_P126.person per
    ON  ce.person_id  = per.person_id
    AND per.active_ind = 1
WHERE DATEDIFF(DAY, ce.UPDT_DT_TM, GETDATE())
      <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
  AND ce.result_val IS NOT NULL
  AND ce.RESULT_VAL != 'None'
  AND UPPER(ce.result_val) != 'UNABLE TO OBTAIN'
  AND ABS(CHECKSUM(ce.CLINICAL_EVENT_ID) % 10) = 0;
