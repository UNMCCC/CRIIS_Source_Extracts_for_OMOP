-- ============================================================
-- OMOP Incremental Procedure Occurrence (MS SQL Server)
-- Translated from Vertica dialect
--
-- Key translation notes:
--   datediff('day', dt1, dt2) -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                 -> GETDATE()
--   ::varchar                 -> TRY_CAST(... AS VARCHAR(MAX))
--   ORDER BY in subqueries    -> removed (invalid without TOP)
--   GROUP BY in CTE with ORDER BY -> removed ORDER BY
-- ============================================================

INSERT INTO UNMHSC_EDW_OMOP.OMOP_INCREMENTAL_PROCEDURE_OCCUR (
    identity_context,
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_end_date,
    procedure_end_datetime,
    procedure_type_concept_id,
    modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_value,
    procedure_source_concept_id,
    modifier_source_value,
    updt_dt_tm
)
SELECT DISTINCT
    'UNMHSC_EDW_MILL_CDS.MD_F_PROCEDURE'        AS identity_context,
    p.PROCEDURE_ID                              AS PROCEDURE_OCCURRENCE_ID,
    p.PERSON_ID,
    p.PROCEDURE_RAW_CODE                        AS PROCEDURE_CONCEPT_ID,
    p.PROCEDURE_DATE,
    p.procedure_dt_tm                           AS PROCEDURE_DATETIME,
    p.procedure_dt_tm                           AS PROCEDURE_END_DATE,
    p.procedure_dt_tm                           AS PROCEDURE_END_DATETIME,
    NULL                                        AS PROCEDURE_TYPE_CONCEPT_ID,
    TRY_CAST(mc.MODIFIER_CODE AS VARCHAR(MAX))  AS MODIFIER_CONCEPT_ID,
    1                                           AS QUANTITY,
    p.RESPONSIBLE_PROVIDER_ID                   AS PROVIDER_ID,
    p.encounter_id                              AS VISIT_OCCURRENCE_ID,
    vd.VISIT_DETAIL_ID,
    p.PROCEDURE_RAW_CODE                        AS PROCEDURE_SOURCE_VALUE,
    p.PROCEDURE_RAW_CODE                        AS PROCEDURE_SOURCE_CONCEPT_ID,
    TRY_CAST(mc.MODIFIER_CODE AS VARCHAR(MAX))  AS MODIFIER_SOURCE_VALUE,
    p.updt_dt_tm
FROM UNMHSC_EDW_MILL_CDS.MD_F_PROCEDURE p
LEFT JOIN (
    SELECT DISTINCT
        m.source_id,
        m.empi_id,
        m.MODIFIER_CODE
    FROM (
        SELECT DISTINCT
            m.source_id,
            m.empi_id,
            m.MODIFIER_CODE,
            ROW_NUMBER() OVER (
                PARTITION BY m.EMPI_ID, m.PROCEDURE_ID
            ) AS procedure_r
        FROM PH_F_PROCEDURE_MODIFIER_CODE m
        WHERE POPULATION_ID = '5b3c88fb-8f30-40f7-a709-87d6486b2732'
          AND source_type   = 'EMR'
          AND LEN(m.MODIFIER_CODE) = 2
        GROUP BY
            m.source_id,
            m.empi_id,
            m.MODIFIER_CODE,
            m.procedure_id
    ) m
    WHERE procedure_r = 1
) mc ON p.PROCEDURE_ID = mc.SOURCE_ID
LEFT JOIN UNMHSC_EDW_OMOP.UA_OMOP_VISIT_DETAIL vd
    ON  p.ENCOUNTER_ID           = vd.VISIT_OCCURRENCE_ID
    AND p.responsible_provider_id = vd.PROVIDER_ID
WHERE DATEDIFF(DAY, p.UPDT_DT_TM, GETDATE())
      <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
  AND p.PROCEDURE_SOURCE_NAME = 'UNIV_NM_HIM';
