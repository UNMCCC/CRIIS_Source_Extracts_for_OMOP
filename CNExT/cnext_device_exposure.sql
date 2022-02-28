/* 
Notes:
1)  First 2 attributes are included so lineage tracing and source primary key 
    identification is clearly contained in the prospective extract file. 

2)  The output IDENTITY_CONTEXT clearly indicates the primary source of the
    extract file.

3)  The output SOURCE_PK will always contain the column(s) which identify
    the primary key for the source system of record.  In cases where source 
	keys include more than one element they should be sequentially numbered,
	i.e. SOURCE_PK_01, SOURCE_PK_02,etc

4)  In instances where there is no equivalent concept in the source table(s),
    simply include a "NULL as MISSING_COLUMN_NAME" in the SQL so it it clear
	that the omission is not an oversight.

5)  The structure will largely resemble the structure of the target table and 
    wherever practical contain all source elements needed to populate the 
	target, excluding OMOP metadata keys which will be looked up during the 
	Staging to Persistent Store ETL operation.
	
6)  Single value sub-selects are recommended for external lookups vs. lots of
    joins.  Makes everything easier to read.  Additionally, all single element 
	sub selects must include TOP 1 for SQLServer or ROWNUM < 2 for Oracle instances.
	This simply protects against SQLCODE -1427 in cases where the source data 
	may not be properly constrained.

7)  Always include a WHERE clause even if it only validates the source
    PK is not null.


8)  Include as much commenting as needed to clearly express the work being 
    performed.
	
9)  This script is to be run and is written to be run in MS SQL Server and will generate errors if run in other DB platforms

10) Comments reflect Item # as referrd to in the NAACCR layout V21-Chapter-IX-

LTV - 1/31/2022 - adding patient's MRN at the end of the query per Mark.

LTV - 2/4/2022 - handled NULL values with the ISNULL function. Handled empty space in F07799 with a case statement.

LTV - 2/8/2022 - handled NULL and empty values for RAD.F07799 in predicate
*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|DEVICE_EXPOSURE_ID|PERSON_ID|DEVICE_CONCEPT_ID|Radiation_Date_Started|Radiation_Datetime_Started|Radiation_Date_Ended|Radiation_Datetime_Ended|DEVICE_TYPE_CONCEPT_ID|UNIQUE_DEVICE_ID|QUANTITY|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|DEVICE_SOURCE_VALUE|DEVICE_SOURCE_CONCEPT_ID|MRN|Modified_DtTm';
SELECT  'CNEXT RADIATION(OMOP_DEVICE_EXPOSURE)' AS IDENTITY_CONTEXT
        ,RAD.uk AS SOURCE_PK
        ,RAD.uk AS DEVICE_EXPOSURE_ID
    	,ISNULL(HSP.F00016, '') AS PERSON_ID
        ,RAD.F07799 AS DEVICE_CONCEPT_ID   --nulls handled by the predicate                                        /*1506*/
		,ISNULL(FORMAT(TRY_CAST(F05187 AS DATE), 'yyyy-MM-dd'), '') AS DEVICE_EXPOSURE_START_DATE                      /*1210*/
        ,ISNULL(FORMAT(TRY_CAST(F05187 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS DEVICE_EXPOSURE_START_DATETIME      /*1210*/
        ,ISNULL(FORMAT(TRY_CAST(F05212 AS DATE), 'yyyy-MM-dd'), '') AS DEVICE_EXPOSURE_END_DATE                        /*3220*/
        ,ISNULL(FORMAT(TRY_CAST(F05212 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS DEVICE_EXPOSURE_END_DATETIME        /*3220*/
	    ,'EHR dispensing record' AS DEVICE_TYPE_CONCEPT_ID
        ,ISNULL(RAD.F05259, '') AS UNIQUE_DEVICE_ID                                                                /*1570*/
        ,ISNULL(RAD.F07797, '') AS QUANTITY                                                                        /*1533*/
        ,ISNULL(RAD.F05156, '') AS PROVIDER_ID                                                                     /*2480*/
        ,HSP.UK AS VISIT_OCCURRENCE_ID
		,TUM.uk AS VISIT_DETAIL_ID
        ,RAD.F07799 AS DEVICE_SOURCE_VALUE   --nulls handled by the predicate                                      /*1502*/
	    ,'1506@'  + RAD.F07799 AS DEVICE_SOURCE_CONCEPT_ID --nulls and empty spaces handled in predicate
        ,ISNULL(HSP.F00006, '') AS MRN
		,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-mm-dd HH:mm:ss'),'')  AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Radiation RAD
  INNER JOIN UNM_CNExTCases.dbo.Tumor TUM ON RAD.fk2 = TUM.uk
  INNER JOIN UNM_CNExTCases.dbo.Hospital HSP ON TUM.uk = HSP.fk2
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK=HExt.UK
  WHERE F05257 > '000'
    AND RAD.F07799 > '00'
	AND RAD.F07799 < '99'
  order by F05212 desc