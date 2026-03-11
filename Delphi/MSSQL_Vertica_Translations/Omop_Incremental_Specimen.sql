-- ============================================================
-- OMOP Incremental Specimen (MS SQL Server)
-- Translated from Vertica dialect
--
-- Key translation notes:
--   datediff('day', dt1, dt2) -> DATEDIFF(DAY, dt1, dt2)
--   getdate()                 -> GETDATE()
--   hash(x)                   -> CHECKSUM(x)
--   || (concat)               -> CONCAT() or +
--   ORDER BY in CTEs removed  (invalid without TOP in SQL Server)
-- ============================================================

;WITH orders AS (
    SELECT
        o.ORDER_ID,
        o.ENCNTR_ID,
        o.PERSON_ID,
        o.ORIG_ORDER_DT_TM,
        o.CATALOG_CD,
        o.ORDER_MNEMONIC,
        od.OE_FIELD_MEANING,
        od.OE_FIELD_DISPLAY_VALUE,
        od.OE_FIELD_VALUE,
        o.UPDT_DT_TM
    FROM UNMHSC_P126.ORDERS o
    JOIN UNMHSC_P126.ORDER_DETAIL od
        ON  o.ORDER_ID         = od.ORDER_ID
        AND od.OE_FIELD_MEANING = 'SPECIMEN TYPE'
    WHERE DATEDIFF(DAY, o.UPDT_DT_TM, GETDATE())
          <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
),

PRIMARY_CONDITION AS (
    SELECT *
    FROM (
        SELECT
            d.encounter_id,
            d.PERSON_ID,
            d.condition_raw_code,
            d.condition_raw_display,
            d.RANK_TYPE,
            d.DIAGNOSIS_TYPE_RAW_DISPLAY,
            ROW_NUMBER() OVER (
                PARTITION BY encounter_id, person_id
                ORDER BY encounter_id
            ) AS row_num
        FROM UNMHSC_EDW_MILL_CDS.MD_F_CONDITION d
        WHERE d.rank_type                 = 'Primary'
          AND d.DIAGNOSIS_TYPE_RAW_DISPLAY = 'Final'
          AND d.encounter_id IN (SELECT DISTINCT ENCNTR_ID FROM orders)
    ) a
    WHERE row_num = 1
),

SECONDARY_CONDITION AS (
    SELECT *
    FROM (
        SELECT
            d.encounter_id,
            d.PERSON_ID,
            d.condition_raw_code,
            d.condition_raw_display,
            d.RANK_TYPE,
            d.DIAGNOSIS_TYPE_RAW_DISPLAY,
            ROW_NUMBER() OVER (
                PARTITION BY encounter_id, person_id
                ORDER BY encounter_id
            ) AS row_num
        FROM UNMHSC_EDW_MILL_CDS.MD_F_CONDITION d
        WHERE d.rank_type = 'Secondary'
          AND d.encounter_id IN (SELECT DISTINCT ENCNTR_ID FROM orders)
    ) a
    WHERE row_num = 1
)

SELECT DISTINCT
    Identity_Context,
    specimen_id,
    PERSON_ID,
    specimen_concept_id,
    specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value,
    UPDT_DT_TM
FROM (
    SELECT DISTINCT
        'UNMHSC_P126.Orders'                                        AS Identity_Context,
        CHECKSUM(
            CAST(o.ENCNTR_ID AS NVARCHAR(MAX))
            + o.oe_field_value
        )                                                           AS specimen_id,
        o.PERSON_ID,
        o.oe_field_value                                            AS specimen_concept_id,
        0                                                           AS specimen_type_concept_id,
        o.ORIG_ORDER_DT_TM                                          AS specimen_date,
        o.ORIG_ORDER_DT_TM                                          AS specimen_datetime,
        0                                                           AS quantity,
        0                                                           AS unit_concept_id,
        o.oe_field_value                                            AS anatomic_site_concept_id,
        COALESCE(pc.condition_raw_code, sc.condition_raw_code)      AS disease_status_concept_id,
        o.oe_field_value                                            AS specimen_source_id,
        o.OE_FIELD_DISPLAY_VALUE                                    AS specimen_source_value,
        NULL                                                        AS unit_source_value,
        o.OE_FIELD_DISPLAY_VALUE                                    AS anatomic_site_source_value,
        COALESCE(pc.condition_raw_display, sc.condition_raw_display) AS disease_status_source_value,
        o.UPDT_DT_TM,
        ROW_NUMBER() OVER (
            PARTITION BY o.ENCNTR_ID, o.oe_field_value
            ORDER BY o.ORIG_ORDER_DT_TM
        )                                                           AS spec_rank
    FROM ORDERS o
    LEFT JOIN PRIMARY_CONDITION pc
        ON pc.encounter_id = o.ENCNTR_ID
    LEFT JOIN SECONDARY_CONDITION sc
        ON sc.encounter_id = o.ENCNTR_ID
    WHERE DATEDIFF(DAY, o.UPDT_DT_TM, GETDATE())
          <= (SELECT CONFIG_VALUE FROM UNMHSC_EDW_OMOP.omop_incremental)
) v
WHERE spec_rank = 1;
