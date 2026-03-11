-- ============================================================
-- OMOP Incremental Visit Occurrence (MS SQL Server)
-- Translated from Vertica dialect
--
-- Contains two operations:
--   1. INSERT  - load incremental visit occurrence records
--   2. UPDATE  - set preceding_visit_occurrence_id using LAG()
--
-- Key translation notes:
--   datediff('day', dt1, dt2) -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                 -> GETDATE()
--   ORDER BY in CTEs removed  (invalid without TOP in SQL Server)
--   GROUP BY positional       -> expanded to column names
-- ============================================================

-- ============================================================
-- 1. INSERT: Load incremental visit occurrence records
-- ============================================================

;WITH encounter_pro AS (
    SELECT
        ENCOUNTER_ID,
        person_id,
        PRSNL_ID    AS provider_id,
        provider_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ENCOUNTER_ID
                ORDER BY provider_type, BEGIN_EFFECTIVE_DT_TM ASC
            ) AS pro_type_rank
        FROM (
            SELECT DISTINCT
                epr.ENCOUNTER_ID,
                epr.PERSON_ID,
                epr.BEGIN_EFFECTIVE_DT_TM,
                epr.PRSNL_ID,
                CASE epr.RELATIONSHIP_TYPE_RAW_DISPLAY
                    WHEN 'Attending Physician'    THEN epr.RELATIONSHIP_TYPE_RAW_DISPLAY
                    WHEN 'ER Attending Physician' THEN epr.RELATIONSHIP_TYPE_RAW_DISPLAY
                    ELSE NULL
                END AS provider_type
            FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER_PERSONNEL_RELTN epr
        ) a
    ) b
    WHERE pro_type_rank = 1
    GROUP BY
        ENCOUNTER_ID,
        person_id,
        PRSNL_ID,
        provider_type
)

INSERT INTO UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_VISIT_OCCERRENC (
    identity_context,
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitted_from_concept_id,
    admitted_from_source_value,
    discharged_to_concept_id,
    discharged_to_source_value,
    preceding_visit_occurrence_id,
    updt_dt_tm
)
SELECT
    'UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER'    AS Identity_Context,
    e.ENCOUNTER_ID                          AS visit_occurrence_id,
    e.person_id,
    e.CLASSIFICATION_raw_CODE               AS visit_concept_id,
    e.REGISTRATION_dt_tm                    AS visit_start_date,
    e.REGISTRATION_DT_TM                    AS visit_start_datetime,
    e.DISCHARGE_dt_tm                       AS visit_end_date,
    e.DISCHARGE_DT_TM                       AS visit_end_datetime,
    0                                       AS visit_type_concept_id,
    ep.provider_id,
    e.FACILITY_SOURCE_ID                    AS care_site_id,
    e.classification_raw_code               AS visit_source_value,
    e.classification_raw_code               AS visit_source_concept_id,
    e.ADMISSION_TYPE_raw_CODE               AS admitted_from_concept_id,
    e.ADMISSION_TYPE_raw_CODE               AS admitted_from_source_value,
    e.DISCHARGE_LOCATION_raw_CODE           AS discharged_to_concept_id,
    e.discharge_location_raw_code           AS discharged_to_source_value,
    NULL                                    AS preceding_visit_occurrence_id,
    e.updt_dt_tm
FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER e
LEFT JOIN encounter_pro ep
    ON e.ENCOUNTER_ID = ep.encounter_id
WHERE DATEDIFF(DAY, e.UPDT_DT_TM, GETDATE())
      <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
GROUP BY
    e.ENCOUNTER_ID,
    e.PERSON_ID,
    e.classification_raw_code,
    e.REGISTRATION_DT_TM,
    e.DISCHARGE_DT_TM,
    e.FACILITY_SOURCE_ID,
    ep.provider_id,
    e.DISCHARGE_LOCATION_raw_CODE,
    e.ADMISSION_TYPE_raw_CODE,
    e.updt_dt_tm;


-- ============================================================
-- 2. UPDATE: Set preceding_visit_occurrence_id using LAG()
-- ============================================================
UPDATE vo
SET    vo.preceding_visit_occurrence_id = upd.past_visit
FROM   UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_VISIT_OCCERRENC vo
JOIN (
    SELECT
        ENCOUNTER_ID,
        person_id,
        REGISTRATION_DT_TM,
        LAG(ENCOUNTER_ID, 1) OVER (
            PARTITION BY person_id
            ORDER BY REGISTRATION_DT_TM ASC
        ) AS Past_visit
    FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER
    GROUP BY
        ENCOUNTER_ID,
        person_id,
        REGISTRATION_DT_TM
) upd ON vo.visit_occurrence_id = upd.ENCOUNTER_ID
      AND vo.person_id           = upd.person_id;
