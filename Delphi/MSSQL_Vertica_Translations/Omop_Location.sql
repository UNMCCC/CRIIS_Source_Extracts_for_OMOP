-- ============================================================
-- OMOP Location (MS SQL Server)
-- Translated from Vertica dialect
--
-- Key translation notes:
--   :: cast      -> TRY_CAST(... AS ...)
--   ILIKE        -> LIKE  (case-insensitive collation assumed)
--   HASH()       -> CHECKSUM()
--   || (concat)  -> CONCAT() / +
--   ORDER BY in CTEs removed (not valid without TOP in SQL Server)
--   CASE LENGTH() -> CASE LEN()
-- ============================================================

WITH person_address AS (
    SELECT
        'UNMHSC_EDW_MILL_CDS.MD_D_PERSON_ADDRESS' AS Identity_Context,
        b.LOCATION_ID,
        b.ADDRESS_1,
        b.ADDRESS_2,
        b.city,
        b.STATE,
        b.ZIP,
        b.COUNTY,
        b.LOCATION_SOURCE_VALUE,
        b.COUNTRY_CONCEPT_ID,
        b.COUNTRY_SOURCE_VALUE,
        b.LATITUDE,
        b.LONGITUDE,
        b.UPDT_DT_TM
    FROM (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY LOCATION_ID
                ORDER BY UPDT_DT_TM DESC
            )                                     AS current_updt,
            LOCATION_ID,
            ADDRESS_1,
            address_2,
            city,
            STATE,
            zip,
            county,
            LOCATION_SOURCE_VALUE,
            COUNTRY_CONCEPT_ID,
            COUNTRY_SOURCE_VALUE,
            LATITUDE,
            LONGITUDE,
            UPDT_DT_TM
        FROM (
            SELECT DISTINCT
                pa.address_id                                   AS LOCATION_ID,
                pa.ADDRESS_LINE_1                               AS ADDRESS_1,
                pa.ADDRESS_LINE_2                               AS ADDRESS_2,
                pa.city,
                pa.STATE_RAW_DISPLAY                            AS STATE,
                LEFT(pa.POSTAL_CODE, 5)                         AS zip,
                NULL                                            AS county,
                pa.address_id                                   AS LOCATION_SOURCE_VALUE,
                TRY_CAST(pa.COUNTRY_RAW_CODE AS VARCHAR(MAX))   AS COUNTRY_CONCEPT_ID,
                pa.COUNTRY_RAW_DISPLAY                          AS COUNTRY_SOURCE_VALUE,
                0.0                                             AS LATITUDE,
                0.0                                             AS LONGITUDE,
                pa.updt_raw_dt_tm                               AS UPDT_DT_TM
            FROM UNMHSC_EDW_MILL_CDS.MD_D_PERSON_ADDRESS pa
            JOIN UNMHSC_EDW_MILL_CDS.MD_D_PERSON p
                ON pa.person_id = p.person_id
            WHERE pa.ADDRESS_TYPE_RAW_DISPLAY IN (
                    'home', 'mailing', 'billing', 'Bill To'
                )
              AND pa.current_ind = 1
        ) a
    ) b
    WHERE current_updt = 1
),

Caresite_ADDRESS AS (
    SELECT DISTINCT
        b.Identity_Context,
        b.LOCATION_ID,
        b.ADDRESS_1,
        b.ADDRESS_2,
        b.CITY,
        b.STATE,
        b.ZIP,
        b.COUNTY,
        b.LOCATION_SOURCE_VALUE,
        b.COUNTRY_CONCEPT_ID,
        b.COUNTRY_SOURCE_VALUE,
        b.LATITUDE,
        b.LONGITUDE,
        b.updt_dt_tm
    FROM (
        SELECT
            e.identity_context,
            CHECKSUM(
                CAST(a.PARENT_ENTITY_ID AS NVARCHAR(MAX))
                + a.STREET_ADDR
            )                                               AS LOCATION_ID,
            a.STREET_ADDR                                   AS ADDRESS_1,
            a.STREET_ADDR2                                  AS ADDRESS_2,
            a.CITY                                          AS CITY,
            CASE LEN(a.STATE)
                WHEN 2 THEN a.STATE
            END                                             AS STATE,
            TRY_CAST(a.ZIPCODE AS VARCHAR(20))             AS ZIP,
            NULL                                            AS COUNTY,
            CHECKSUM(
                a.STREET_ADDR
                + TRY_CAST(a.ZIPCODE AS VARCHAR(20))
            )                                               AS LOCATION_SOURCE_VALUE,
            NULL                                            AS COUNTRY_CONCEPT_ID,
            NULL                                            AS COUNTRY_SOURCE_VALUE,
            0.0                                             AS LATITUDE,
            0.0                                             AS LONGITUDE,
            a.updt_dt_tm
        FROM UNMHSC_EDW_OMOP.OMOP_CARE_SITE e
        LEFT JOIN (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY a.PARENT_ENTITY_ID
                       ORDER BY a.ADDRESS_TYPE_SEQ, a.updt_dt_tm DESC
                   ) AS updt_ord
            FROM UNMHSC_P126.ADDRESS a
            WHERE PARENT_ENTITY_NAME = 'LOCATION'
              AND ADDRESS_TYPE_CD    = '405134056'
        ) a ON e.care_site_id = a.PARENT_ENTITY_ID
            AND a.ACTIVE_IND  = 1
            AND a.updt_ord    = 1
        WHERE a.STREET_ADDR LIKE 'PO%'
           OR LEFT(a.STREET_ADDR, 1) BETWEEN '1' AND '9'
        GROUP BY
            e.care_site_id,
            a.STREET_ADDR,
            a.STREET_ADDR2,
            a.ZIPCODE,
            a.CITY,
            a.STATE,
            a.PARENT_ENTITY_ID,
            a.updt_dt_tm,
            e.identity_context
    ) b
)

SELECT * FROM person_address

UNION

SELECT * FROM Caresite_ADDRESS;
