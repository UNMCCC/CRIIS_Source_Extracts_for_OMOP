-- ============================================================
-- OMOP Care Site (MS SQL Server)
-- Translated from Vertica dialect
--
-- Key translation notes:
--   ILIKE       -> LIKE  (assumes case-insensitive DB collation;
--                          wrap in LOWER() if collation is CS)
--   HASH()      -> CHECKSUM()
--   ::TIMESTAMP -> TRY_CAST(... AS DATETIME2)
--   || (concat) -> + or CONCAT()
--   GROUP BY positional -> expanded to column names
-- ============================================================

SELECT
    'UNMHSC_P126.ADDRESS'                                       AS Identity_Context,
    csite.PARENT_ENTITY_ID                                      AS CARE_SITE_ID,
    csite.DESCRIPTION                                           AS care_site_name,
    0                                                           AS place_of_service_concept_id,
    CHECKSUM(
        CAST(csite.PARENT_ENTITY_ID AS NVARCHAR(MAX))
        + csite.STREET_ADDR
    )                                                           AS LOCATION_ID,
    csite.DESCRIPTION                                           AS care_site_source_value,
    CASE csite.DESCRIPTION
        WHEN 'UNMHSC/Cancer Center'          THEN 'Outpatient'
        WHEN 'UNMHSC/CASAA + Milagro Programs' THEN 'Outpatient'
        ELSE NULL
    END                                                         AS place_of_service_source_value,
    TRY_CAST(csite.updt_dt_tm AS DATETIME2)                    AS UPDT_DT_TM
FROM (
    SELECT
        e.LOC_FACILITY_CD,
        cv.DESCRIPTION,
        a.STREET_ADDR,
        a.STREET_ADDR2,
        a.ZIPCODE,
        a.CITY,
        a.STATE,
        a.PARENT_ENTITY_ID,
        a.updt_dt_tm
    FROM UNMHSC_P126.ENCOUNTER e
    LEFT JOIN (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY a.PARENT_ENTITY_ID
                   ORDER BY a.ADDRESS_TYPE_SEQ, a.updt_dt_tm DESC
               ) AS updt_ord
        FROM UNMHSC_P126.ADDRESS a
        WHERE PARENT_ENTITY_NAME = 'LOCATION'
          AND ADDRESS_TYPE_CD    = '405134056'
          AND PARENT_ENTITY_ID   NOT IN ('1133767')
    ) a ON e.LOC_FACILITY_CD = a.PARENT_ENTITY_ID
        AND a.ACTIVE_IND      = 1
        AND a.updt_ord        = 1
    LEFT JOIN UNMHSC_P126.CODE_VALUE cv
        ON e.LOC_FACILITY_CD = cv.CODE_VALUE
    WHERE a.STREET_ADDR LIKE 'PO%'
       OR LEFT(a.STREET_ADDR, 1) BETWEEN '1' AND '9'
    GROUP BY
        e.LOC_FACILITY_CD,
        cv.DESCRIPTION,
        a.STREET_ADDR,
        a.STREET_ADDR2,
        a.ZIPCODE,
        a.CITY,
        a.STATE,
        a.PARENT_ENTITY_ID,
        a.updt_dt_tm
) csite
GROUP BY
    csite.PARENT_ENTITY_ID,
    csite.DESCRIPTION,
    csite.STREET_ADDR,
    csite.updt_dt_tm;
