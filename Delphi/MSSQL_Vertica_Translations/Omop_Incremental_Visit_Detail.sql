-- ============================================================
-- OMOP Incremental Visit Detail (MS SQL Server)
-- Translated from Vertica dialect
--
-- Contains two operations:
--   1. INSERT  - load incremental visit detail records
--   2. UPDATE  - set preceding_visit_detail_id using LAG()
--
-- Key translation notes:
--   (prsnl_id || encounter_id)::int -> TRY_CAST(CONCAT() AS BIGINT)
--   datediff('day', dt1, dt2)       -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                       -> GETDATE()
--   BETWEEN on datetime range       -> same in SQL Server
--   GROUP BY positional             -> expanded to column names
-- ============================================================

-- ============================================================
-- 1. INSERT: Load incremental visit detail records
-- ============================================================

;WITH visit AS (
    SELECT
        TRY_CAST(
            CAST(epr.prsnl_id AS NVARCHAR(50))
            + CAST(e.ENCOUNTER_ID AS NVARCHAR(50))
        AS BIGINT)                                  AS visit_detail_id,
        e.PERSON_ID,
        0                                           AS visit_detail_concept_id,
        MIN(epr.BEGIN_EFFECTIVE_dt_tm)              AS VISIT_detail_START_DATE,
        MIN(epr.BEGIN_EFFECTIVE_DT_TM)              AS VISIT_detail_START_DATETIME,
        e.DISCHARGE_dt_tm                           AS VISIT_detail_END_DATE,
        e.DISCHARGE_DT_TM                           AS VISIT_detail_END_DATETIME,
        0                                           AS visit_detail_type_concept_id,
        epr.PRSNL_ID                                AS provider_id,
        e.FACILITY_SOURCE_ID                        AS CARE_SITE_ID,
        e.CLASSIFICATION_raw_CODE                   AS visit_detail_source_value,
        e.CLASSIFICATION_raw_CODE                   AS visit_detail_source_concept_id,
        0                                           AS PRECEDING_VISIT_OCCURRENCE_ID,
        e.ADMISSION_TYPE_raw_CODE                   AS admitted_from_concept_id,
        e.ADMISSION_TYPE_raw_CODE                   AS admitted_from_source_value,
        e.DISCHARGE_LOCATION_raw_CODE               AS discharged_to_source_value,
        e.DISCHARGE_LOCATION_raw_CODE               AS discharged_to_concept_id,
        0                                           AS PRECEDING_VISIT_detail_id,
        e.ENCOUNTER_ID                              AS VISIT_OCCURRENCE_ID,
        0                                           AS RELATIONSHIP_TYPE_RAW_CODE,
        MIN(epr.BEGIN_EFFECTIVE_DT_TM)              AS BEGIN_EFFECTIVE_DT_TM,
        MAX(epr.updt_dt_tm)                         AS updt_dt_tm
    FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER e
    JOIN (
        SELECT
            epr.encounter_Id,
            epr.PRSNL_ID,
            epr.person_id,
            MIN(epr.BEGIN_EFFECTIVE_DT_TM) AS BEGIN_EFFECTIVE_DT_TM,
            MAX(epr.updt_dt_tm)            AS updt_dt_tm
        FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER_PERSONNEL_RELTN epr
        GROUP BY
            epr.encounter_Id,
            epr.PRSNL_ID,
            epr.person_id
    ) epr ON e.ENCOUNTER_ID = epr.ENCOUNTER_ID
          AND e.PERSON_ID   = epr.PERSON_ID
          AND epr.BEGIN_EFFECTIVE_DT_TM BETWEEN e.REGISTRATION_DT_TM AND e.DISCHARGE_DT_TM
    WHERE DATEDIFF(DAY, epr.UPDT_DT_TM, GETDATE())
          <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
    GROUP BY
        TRY_CAST(
            CAST(epr.prsnl_id AS NVARCHAR(50))
            + CAST(e.ENCOUNTER_ID AS NVARCHAR(50))
        AS BIGINT),
        e.PERSON_ID,
        e.DISCHARGE_dt_tm,
        e.DISCHARGE_DT_TM,
        e.FACILITY_SOURCE_ID,
        epr.PRSNL_ID,
        e.CLASSIFICATION_raw_CODE,
        e.ADMISSION_TYPE_raw_CODE,
        e.DISCHARGE_LOCATION_raw_CODE,
        e.ENCOUNTER_ID,
        epr.BEGIN_EFFECTIVE_DT_TM
)

INSERT INTO UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_VISIT_DETAIL (
    identity_context,
    visit_detail_id,
    person_id,
    visit_detail_concept_id,
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    visit_detail_type_concept_id,
    provider_id,
    care_site_id,
    visit_detail_source_value,
    visit_detail_source_concept_id,
    preceding_visit_occurrence_id,
    admitted_from_concept_id,
    admitted_from_source_value,
    discharged_to_source_value,
    discharged_to_concept_id,
    preceding_visit_detail_id,
    visit_occurrence_id,
    updt_dt_tm
)
SELECT
    'UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER'    AS Identity_Context,
    a.visit_detail_id,
    a.person_Id,
    a.visit_detail_concept_id,
    a.VISIT_detail_START_DATE,
    a.VISIT_detail_START_DATETIME,
    a.VISIT_detail_END_DATE,
    a.VISIT_detail_END_DATETIME,
    a.visit_detail_type_concept_id,
    a.provider_id,
    a.CARE_SITE_ID,
    a.visit_detail_source_value,
    a.visit_detail_source_concept_id,
    vo.PRECEDING_VISIT_OCCURRENCE_ID,
    vo.admitted_from_concept_id,
    vo.admitted_from_source_value,
    vo.discharged_to_source_value,
    vo.discharged_to_concept_id,
    a.PRECEDING_VISIT_detail_id,
    vo.VISIT_OCCURRENCE_ID,
    a.UPDT_DT_TM
FROM UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_VISIT_OCCERRENC vo
JOIN visit a ON a.visit_occurrence_id = vo.VISIT_OCCURRENCE_ID
WHERE DATEDIFF(DAY, a.UPDT_DT_TM, GETDATE())
      <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental);


-- ============================================================
-- 2. UPDATE: Set preceding_visit_detail_id using LAG()
-- ============================================================
UPDATE vd
SET    vd.preceding_visit_detail_id = upd.preceding_visit_detail_id
FROM   UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_VISIT_DETAIL vd
JOIN (
    SELECT
        visit_occurrence_id,
        visit_detail_id,
        LAG(visit_detail_id, 1) OVER (
            PARTITION BY visit_occurrence_id
            ORDER BY visit_detail_start_date ASC
        ) AS PRECEDING_VISIT_detail_id
    FROM UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_VISIT_DETAIL
) upd ON vd.visit_occurrence_id = upd.visit_occurrence_id
      AND vd.visit_detail_id    = upd.visit_detail_id;
