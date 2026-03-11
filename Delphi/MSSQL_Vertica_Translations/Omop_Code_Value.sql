-- ============================================================
-- OMOP Code Value (MS SQL Server)
-- Translated from Vertica dialect
--
-- Loads reference/lookup code values for multiple OMOP domains:
--   1. Person codes     (Gender, Race, Ethnicity)
--   2. Provider codes   (Taxonomy)
--   3. Visit codes      (Visit Type, Admission Type, Discharge Type)
--   4. Condition codes  (Condition Type, Condition Status)
--   5. Drug codes       (Drug, Route, Unit, Stop, Immunization)
--   6. Procedure codes  (Procedure, Modifier)
--   7. Measurement codes (Result, Unit)
--
-- Key translation notes:
--   ::VARCHAR           -> TRY_CAST(... AS VARCHAR(MAX))
--   ascii()             -> ASCII()
--   CASE WHEN x = NULL -> CASE WHEN x IS NULL (standard SQL)
--   ORDER BY in sub-    -> removed (invalid without TOP in SQL Server)
--   queries
-- ============================================================

-- ============================================================
-- 1. Person codes: Gender, Race, Ethnicity
-- ============================================================
INSERT INTO UNMHSC_EDW_OMOP.OMOP_CODE_VALUE (
    code_id, code_display, code_type, table_used, column_used
)
SELECT *
FROM (
    SELECT DISTINCT
        TRY_CAST(GENDER_CODE AS VARCHAR(MAX))               AS CODE_ID,
        GENDER_RAW_DISPLAY                                  AS CODE_DISPLAY,
        'Gender'                                            AS CODE_TYPE,
        'OMOP Person'                                       AS Table_Used,
        'Gender Concept id; Gender Source Value; Gender Source Concept id'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSON

    UNION

    SELECT DISTINCT
        TRY_CAST(RACE_RAW_CODE AS VARCHAR(MAX))             AS CODE_ID,
        RACE_RAW_DISPLAY                                    AS CODE_DISPLAY,
        'Race'                                              AS CODE_TYPE,
        'OMOP Person'                                       AS Table_Used,
        'Race Concept id; Race Source Value; Race Source Concept id'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSON

    UNION

    SELECT DISTINCT
        TRY_CAST(ETHNICITY_raw_CODE AS VARCHAR(MAX))        AS CODE_ID,
        ETHNICITY_RAW_DISPLAY                               AS CODE_DISPLAY,
        'Ethnicity'                                         AS CODE_TYPE,
        'OMOP Person'                                       AS Table_Used,
        'Ethnicity Concept id; Ethnicity Source Value; Ethnicity Source Concept id'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSON
) person
WHERE person.code_id      IS NOT NULL
  AND person.code_display IS NOT NULL;


-- ============================================================
-- 2. Provider codes: NPI Taxonomy
-- ============================================================
INSERT INTO UNMHSC_EDW_OMOP.OMOP_CODE_VALUE (
    code_id, code_display, code_type, table_used, column_used
)
SELECT *
FROM (
    SELECT DISTINCT
        TRY_CAST(TAXONOMY_CODE AS VARCHAR(MAX))             AS CODE_ID,
        CASE
            WHEN taxonomy_specialization IS NULL  THEN TAXONOMY_CLASSIFICATION
            WHEN taxonomy_specialization = ''     THEN TAXONOMY_CLASSIFICATION
            ELSE taxonomy_specialization
        END                                                 AS CODE_DISPLAY,
        'Provider Taxonomy'                                 AS CODE_TYPE,
        'OMOP Provider'                                     AS Table_Used,
        'Specialty Concept id; Specialty Source Concept id' AS Column_Used
    FROM reference.NPPES_TAXONOMY
) provider;


-- ============================================================
-- 3. Visit codes: Visit Type, Admission Type, Discharge Type
-- ============================================================
INSERT INTO UNMHSC_EDW_OMOP.OMOP_CODE_VALUE (
    code_id, code_display, code_type, table_used, column_used
)
SELECT *
FROM (
    SELECT DISTINCT
        TRY_CAST(CLASSIFICATION_raw_CODE AS VARCHAR(MAX))   AS CODE_ID,
        CLASSIFICATION_RAW_DISPLAY                          AS CODE_DISPLAY,
        'Visit Type'                                        AS Code_type,
        'OMOP Visit Occurrence; OMOP Visit Detail'          AS Table_Used,
        'Visit Concept Id; Visit Source Value; Visit Source Concept Id'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER

    UNION

    SELECT DISTINCT
        TRY_CAST(Admission_type_raw_code AS VARCHAR(MAX))   AS CODE_ID,
        ADMISSION_TYPE_RAW_DISPLAY                          AS CODE_DISPLAY,
        'Admission Type'                                    AS Code_type,
        'OMOP Visit Occurrence; OMOP Visit Detail'          AS Table_Used,
        'Admitted From Concept id; Admitted From Source Value'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER

    UNION

    SELECT DISTINCT
        TRY_CAST(Discharge_location_raw_code AS VARCHAR(MAX)) AS CODE_ID,
        DISCHARGE_LOCATION_RAW_DISPLAY                      AS CODE_DISPLAY,
        'Discharge Type'                                    AS Code_type,
        'OMOP Visit Occurrence; OMOP Visit Detail'          AS Table_Used,
        'Discharged To Concept Id; Discharged To Source Value'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER
) vo
WHERE vo.code_id      IS NOT NULL
  AND vo.code_display IS NOT NULL;


-- ============================================================
-- 4. Condition codes: Condition Type, Condition Status
-- ============================================================
INSERT INTO UNMHSC_EDW_OMOP.OMOP_CODE_VALUE (
    code_id, code_display, code_type, table_used, column_used
)
SELECT DISTINCT
    CODE_ID,
    CODE_DISPLAY,
    Code_type,
    Table_Used,
    Column_Used
FROM (
    SELECT DISTINCT
        c.CONDITION_RAW_CODE                                AS CODE_ID,
        ASCII(COALESCE(co.condition_primary_display, c.condition_raw_display))
                                                            AS code_1,
        COALESCE(co.condition_primary_display, c.condition_raw_display)
                                                            AS CODE_DISPLAY,
        'Condition Type'                                    AS Code_type,
        'OMOP Condition Occurrence'                         AS Table_Used,
        'Condition Concept Id; Condition Source Value; Condition Source Concept Id'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_CONDITION c
    LEFT JOIN PH_F_CONDITION co
        ON  c.CONDITION_CODING_SYSTEM_ID = co.CONDITION_RAW_CODING_SYSTEM_ID
        AND c.CONDITION_RAW_CODE         = co.CONDITION_RAW_CODE
    WHERE ASCII(c.CONDITION_RAW_CODE) != 32
) a
WHERE a.code_1 != 32

UNION

SELECT DISTINCT
    TRY_CAST(COALESCE(STATUS_RAW_CODE, DIAGNOSIS_TYPE_RAW_CODE) AS VARCHAR(MAX))
                                                            AS CODE_ID,
    COALESCE(STATUS_RAW_DISPLAY, DIAGNOSIS_TYPE_RAW_DISPLAY) AS CODE_DISPLAY,
    'Condition Status'                                      AS code_type,
    'OMOP Condition Occurrence'                             AS Table_Used,
    'Condition Status Concept Id; Condition Status Source Value'
                                                            AS Column_Used
FROM UNMHSC_EDW_MILL_CDS.MD_F_CONDITION
WHERE COALESCE(STATUS_RAW_CODE, DIAGNOSIS_TYPE_RAW_CODE) IS NOT NULL
  AND COALESCE(STATUS_RAW_DISPLAY, DIAGNOSIS_TYPE_RAW_DISPLAY) IS NOT NULL;


-- ============================================================
-- 5. Drug codes: Drug, Route, Unit, Stop Reason, Immunization
-- ============================================================
INSERT INTO UNMHSC_EDW_OMOP.OMOP_CODE_VALUE (
    code_id, code_display, code_type, table_used, column_used
)
SELECT *
FROM (
    SELECT DISTINCT
        TRY_CAST(drug_raw_code AS VARCHAR(MAX))             AS CODE_ID,
        drug_RAW_DISPLAY                                    AS CODE_DISPLAY,
        'Drug Type'                                         AS Code_type,
        'OMOP Drug Exposure'                                AS Table_Used,
        'Drug Concept Id; Drug Source Value; Drug Source Concept Id'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_MEDICATION

    UNION

    SELECT DISTINCT
        TRY_CAST(route_raw_code AS VARCHAR(MAX))            AS CODE_ID,
        route_RAW_DISPLAY                                   AS CODE_DISPLAY,
        'Drug Route Type'                                   AS Code_type,
        'OMOP Drug Exposure'                                AS Table_Used,
        'Route Concept Id; Route Source Value'              AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_MEDICATION
    WHERE route_raw_code    IS NOT NULL
      AND route_raw_display IS NOT NULL

    UNION

    SELECT DISTINCT
        TRY_CAST(dose_unit_raw_code AS VARCHAR(MAX))        AS CODE_ID,
        dose_unit_RAW_DISPLAY                               AS CODE_DISPLAY,
        'Drug Unit Type'                                    AS Code_type,
        'OMOP Drug Exposure'                                AS Table_Used,
        'Dose Unit Source Value'                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_MEDICATION

    UNION

    SELECT DISTINCT
        TRY_CAST(stop_type_raw_code AS VARCHAR(MAX))        AS CODE_ID,
        stop_type_RAW_DISPLAY                               AS CODE_DISPLAY,
        'Drug Stop Type'                                    AS Code_type,
        'OMOP Drug Exposure'                                AS Table_Used,
        'Stop Reason'                                       AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_MEDICATION

    UNION

    SELECT DISTINCT
        TRY_CAST(Immunization_raw_code AS VARCHAR(MAX))     AS CODE_ID,
        Immunization_RAW_DISPLAY                            AS CODE_DISPLAY,
        'Drug Type'                                         AS Code_type,
        'OMOP Drug Exposure'                                AS Table_Used,
        'Drug Concept Id; Drug Source Value; Drug Source Concept Id'
                                                            AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_IMMUNIZATION

    UNION

    SELECT DISTINCT
        TRY_CAST(Admin_route_raw_code AS VARCHAR(MAX))      AS CODE_ID,
        admin_route_RAW_DISPLAY                             AS CODE_DISPLAY,
        'Drug Route Type'                                   AS Code_type,
        'OMOP Drug Exposure'                                AS Table_Used,
        'Route Concept Id; Route Source Value'              AS Column_Used
    FROM UNMHSC_EDW_MILL_CDS.MD_F_IMMUNIZATION
) d
WHERE d.code_id      IS NOT NULL
  AND d.code_display IS NOT NULL;


-- ============================================================
-- 6. Procedure codes: Procedure Type, Modifier
-- ============================================================
INSERT INTO UNMHSC_EDW_OMOP.OMOP_CODE_VALUE (
    code_id, code_display, code_type, table_used, column_used
)
SELECT *
FROM (
    SELECT
        CODE_ID,
        CODE_DISPLAY,
        Code_type,
        Table_Used,
        Column_Used
    FROM (
        SELECT DISTINCT
            TRY_CAST(PROCEDURE_RAW_CODE AS VARCHAR(MAX))    AS CODE_ID,
            PROCEDURE_RAW_DISPLAY                           AS CODE_DISPLAY,
            ASCII(procedure_raw_display)                    AS code_1,
            'Procedure Type'                                AS Code_type,
            'OMOP Procedure Occurrence'                     AS Table_Used,
            'Procedure Concept Id; Procedure Source Value; Procedure Source Concept Id'
                                                            AS Column_Used
        FROM UNMHSC_EDW_MILL_CDS.MD_F_PROCEDURE
        WHERE ASCII(procedure_raw_code) != 32
    ) a
    WHERE a.code_1 != 32

    UNION

    SELECT DISTINCT
        TRY_CAST(Modifier_code AS VARCHAR(MAX))             AS CODE_ID,
        Modifier_DISPLAY                                    AS CODE_DISPLAY,
        'Modifier Type'                                     AS Code_type,
        'OMOP Procedure Occurrence'                         AS Table_Used,
        'Modifier Concept Id; Modifier Source Value'        AS Column_Used
    FROM PH_F_PROCEDURE_MODIFIER_CODE
    WHERE source_type      = 'EMR'
      AND MODIFIER_DISPLAY != ''
) pro
WHERE pro.code_id      IS NOT NULL
  AND pro.code_display IS NOT NULL;


-- ============================================================
-- 7. Measurement codes: Result Code, Unit
-- ============================================================
INSERT INTO UNMHSC_EDW_OMOP.OMOP_CODE_VALUE (
    code_id, code_display, code_type, table_used, column_used
)
SELECT *
FROM (
    SELECT DISTINCT
        TRY_CAST(RESULT_CODE AS VARCHAR(MAX))               AS CODE_ID,
        RESULT_PRIMARY_DISPLAY                              AS CODE_DISPLAY,
        'Measure Type'                                      AS Code_type,
        'OMOP Measurement'                                  AS Table_Used,
        'Measurement Concept Id; Measurement Source Value; Measurement Source Concept Id'
                                                            AS Column_Used
    FROM PH_F_RESULT

    UNION

    SELECT DISTINCT
        TRY_CAST(NORM_UNIT_OF_MEASURE_RAW_CODE AS VARCHAR(MAX)) AS CODE_ID,
        COALESCE(NORM_UNIT_OF_MEASURE_PRIMARY_DISPLAY, Norm_unit_of_measure_display)
                                                            AS CODE_DISPLAY,
        'Measure Unit Type'                                 AS Code_type,
        'OMOP Measurement'                                  AS Table_Used,
        'Unit Source Value; Unit Source Concept Id'         AS Column_Used
    FROM PH_F_RESULT
) r
WHERE r.code_id      IS NOT NULL
  AND r.code_display IS NOT NULL;
