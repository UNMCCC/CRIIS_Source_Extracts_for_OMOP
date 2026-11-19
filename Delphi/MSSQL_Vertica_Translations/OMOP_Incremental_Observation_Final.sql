-- ============================================================
-- OMOP Incremental Observation Final (MS SQL Server)
-- Translated from Vertica dialect
--
-- Deduplicates OMOP_INCREMENTAL_OBSERVATION by keeping the
-- most recent record per (person_id, observation_date, description).
--
-- Key translation notes:
--   ifnull()             -> ISNULL()
--   || (concat)          -> CONCAT() or +
--   DATE(observation_date) -> CAST(observation_date AS DATE)
--   ORDER BY inside subquery -> valid in SQL Server with ROW_NUMBER() context
-- ============================================================

INSERT INTO UNMHSC_EDW_OMOP.OMOP_INCR_OBSERVATION_FINAL (
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
SELECT
    IDENTITY_CONTEXT,
    OBSERVATION_ID,
    person_id,
    EVENT_CD,
    observation_date,
    OBSERVATION_DATETIME,
    OBSERVATION_TYPE_CONCEPT_ID,
    FORM_DESCRIPTION,
    SECTION_DESCRIPTION,
    -- Vertica: DESCRIPTION||''||ifnull(secondary_description,'')
    DESCRIPTION + '' + ISNULL(secondary_description, '') AS DESCRIPTION,
    VALUE_AS_NUMBER,
    VALUE_AS_STRING,
    VALUE_AS_CONCEPT_ID,
    QUALIFIER_CONCEPT_ID,
    UNIT_CONCEPT_ID,
    PROVIDER_ID,
    VISIT_OCCURRENCE_ID,
    VISIT_DETAIL_ID,
    OBSERVATION_SOURCE_VALUE,
    OBSERVATION_SOURCE_CONCEPT_ID,
    UNIT_SOURCE_VALUE,
    QUALIFIER_SOURCE_ID,
    VALUE_SOURCE_VALUE,
    OBSERVATION_EVENT_ID,
    OBS_EVENT_FIELD_CONCEPT_ID,
    UPDT_DT_TM
FROM (
    SELECT
        IDENTITY_CONTEXT,
        OBSERVATION_ID,
        person_id,
        EVENT_CD,
        observation_date,
        OBSERVATION_DATETIME,
        OBSERVATION_TYPE_CONCEPT_ID,
        FORM_DESCRIPTION,
        SECTION_DESCRIPTION,
        DESCRIPTION,
        secondary_description,
        VALUE_AS_NUMBER,
        VALUE_AS_STRING,
        VALUE_AS_CONCEPT_ID,
        QUALIFIER_CONCEPT_ID,
        UNIT_CONCEPT_ID,
        PROVIDER_ID,
        VISIT_OCCURRENCE_ID,
        VISIT_DETAIL_ID,
        OBSERVATION_SOURCE_VALUE,
        OBSERVATION_SOURCE_CONCEPT_ID,
        UNIT_SOURCE_VALUE,
        QUALIFIER_SOURCE_ID,
        VALUE_SOURCE_VALUE,
        OBSERVATION_EVENT_ID,
        OBS_EVENT_FIELD_CONCEPT_ID,
        UPDT_DT_TM,
        ROW_NUMBER() OVER (
            PARTITION BY PERSON_ID, CAST(OBSERVATION_DATE AS DATE), DESCRIPTION
            ORDER BY CAST(OBSERVATION_DATE AS DATE) DESC
        ) AS OBS_ORDER
    FROM UNMHSC_EDW_OMOP.OMOP_INCR_OBSERVATION
) A
WHERE OBS_ORDER = 1;
