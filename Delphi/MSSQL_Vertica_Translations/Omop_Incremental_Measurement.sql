-- ============================================================
-- OMOP Incremental Measurement (MS SQL Server)
-- Translated from Vertica dialect
--
-- Contains two operations:
--   1. INSERT  - load incremental measurement records
--   2. UPDATE  - set visit_occurrence_id and visit_detail_id
--
-- Key translation notes:
--   datediff('day', dt1, dt2)       -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                       -> GETDATE()
--   hash(x)                         -> CHECKSUM(x)
--   DATE(x)                         -> CAST(x AS DATE)
--   year(x)                         -> YEAR(x)   (same)
--   split_part(str, ':', 3)::int    -> SUBSTRING/CHARINDEX equivalent
--   ::int                           -> TRY_CAST(... AS INT)
--   coalesce()                      -> COALESCE()  (same)
-- ============================================================

-- Helper: split_part equivalent for 3rd colon-delimited segment
-- split_part(r.ENCOUNTER_ID, ':', 3) is expressed below as a SUBSTRING expression.
-- Assumes ENCOUNTER_ID has format  a:b:c[:...]

-- ============================================================
-- 1. INSERT: Load incremental measurement records
-- ============================================================

;WITH provider AS (
    SELECT
        Measure_id,
        ACTING_PROVIDER_ID,
        source_id,
        NAME_FULL_FORMATTED,
        PERSON_ID,
        EFFECTIVE_YEAR
    FROM UNMHSC_EDW_OMOP.omop_measure_provider_id
    WHERE DATEDIFF(DAY, action_dt_tm, GETDATE())
          <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
)

INSERT INTO UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_MEASUREMENT (
    identity_context,
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_time,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    unit_source_concept_id,
    value_source_value,
    measurement_event_id,
    meas_event_concept_id,
    updt_dt_tm
)
SELECT DISTINCT
    'ph_f_result'                                   AS Identity_Context,
    CHECKSUM(r.result_id)                           AS MEASUREMENT_ID,
    pa.source_id                                    AS PERSON_ID,
    r.RESULT_CODE                                   AS MEASUREMENT_CONCEPT_ID,
    CAST(r.SERVICE_DATE AS DATE)                    AS MEASUREMENT_DATE,
    r.SERVICE_DATE                                  AS MEASUREMENT_DATETIME,
    0                                               AS MEASUREMENT_TIME,
    'EMR'                                           AS MEASUREMENT_TYPE_CONCEPT_ID,
    0                                               AS OPERATOR_CONCEPT_ID,
    r.NORM_NUMERIC_VALUE                            AS VALUE_AS_NUMBER,
    0                                               AS VALUE_AS_CONCEPT_ID,
    TRY_CAST(r.Norm_unit_of_measure_raw_code AS INT) AS UNIT_CONCEPT_ID,
    r.NORM_REF_RANGE_LOW                            AS RANGE_LOW,
    r.NORM_REF_RANGE_HIGH                           AS RANGE_HIGH,
    p.person_id                                     AS PROVIDER_ID,
    -- split_part(r.ENCOUNTER_ID, ':', 3)::int
    -- Extract the 3rd colon-delimited part from ENCOUNTER_ID
    TRY_CAST(
        SUBSTRING(
            r.ENCOUNTER_ID,
            CHARINDEX(':', r.ENCOUNTER_ID,
                CHARINDEX(':', r.ENCOUNTER_ID) + 1
            ) + 1,
            CASE
                WHEN CHARINDEX(':',
                        r.ENCOUNTER_ID,
                        CHARINDEX(':', r.ENCOUNTER_ID,
                            CHARINDEX(':', r.ENCOUNTER_ID) + 1
                        ) + 1
                     ) > 0
                THEN CHARINDEX(':',
                        r.ENCOUNTER_ID,
                        CHARINDEX(':', r.ENCOUNTER_ID,
                            CHARINDEX(':', r.ENCOUNTER_ID) + 1
                        ) + 1
                     )
                     - CHARINDEX(':', r.ENCOUNTER_ID,
                           CHARINDEX(':', r.ENCOUNTER_ID) + 1
                       ) - 1
                ELSE LEN(r.ENCOUNTER_ID)
            END
        ) AS INT
    )                                               AS VISIT_OCCURRENCE_ID,
    0                                               AS VISIT_DETAIL_ID,
    r.RESULT_CODE                                   AS MEASUREMENT_SOURCE_VALUE,
    r.RESULT_CODE                                   AS MEASUREMENT_SOURCE_CONCEPT_ID,
    r.NORM_UNIT_OF_MEASURE_CODE                     AS UNIT_SOURCE_VALUE,
    r.NORM_UNIT_OF_MEASURE_CODE                     AS UNIT_SOURCE_CONCEPT_ID,
    0                                               AS VALUE_SOURCE_VALUE,
    0                                               AS MEASUREMENT_EVENT_ID,
    0                                               AS MEAS_EVENT_CONCEPT_ID,
    COALESCE(r.last_update_dt_tm, r.service_local_dt_tm) AS Updt_dt_tm
FROM PH_F_RESULT r
LEFT JOIN (
    SELECT DISTINCT
        pa.EMPI_ID,
        pa.POPULATION_ID,
        pa.ALIAS_TYPE_DISPLAY,
        pa.ALIAS,
        pa.SOURCE_ID,
        pa.PERSON_SEQ,
        ROW_NUMBER() OVER (
            PARTITION BY empi_id
            ORDER BY PERSON_SEQ DESC
        ) AS person_r
    FROM PH_D_PERSON_ALIAS pa
    WHERE pa.source_type         = 'EMR'
      AND pa.SOURCE_DESCRIPTION  = 'P126.UNIV_NM'
      AND pa.POPULATION_ID       = '5b3c88fb-8f30-40f7-a709-87d6486b2732'
) pa ON r.EMPI_ID       = pa.EMPI_ID
     AND r.POPULATION_ID = pa.POPULATION_ID
     AND pa.person_r     = 1
LEFT JOIN provider p
    ON  p.measure_id    = CHECKSUM(r.result_id)
    AND p.effective_year = YEAR(r.service_date)
WHERE r.SOURCE_TYPE = 'EMR'
  AND DATEDIFF(DAY,
        COALESCE(r.last_update_dt_tm, r.service_local_dt_tm),
        GETDATE()
      ) <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
  AND r.CATEGORY_CODE IN ('vital-signs', 'laboratory')
  AND r.POPULATION_ID = '5b3c88fb-8f30-40f7-a709-87d6486b2732'
  AND r.NORM_NUMERIC_VALUE IS NOT NULL;


-- ============================================================
-- 2. UPDATE: Set visit_occurrence_id and visit_detail_id
-- ============================================================
UPDATE meas
SET    meas.visit_occurrence_id = upd.visit_occurrence_id,
       meas.visit_detail_id     = upd.visit_detail_id
FROM   UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_MEASUREMENT meas
JOIN (
    SELECT
        vd.visit_occurrence_id,
        vd.visit_detail_id
    FROM UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_VISIT_DETAIL vd
) upd ON meas.visit_occurrence_id = upd.visit_occurrence_id;
