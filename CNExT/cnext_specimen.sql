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

*/

SELECT  'CNEXT TUMOR(OMOP_SPECIMEN)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS SPECIMEN_ID                                                                                                                                  /*1772*/
        ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
        ,'740@' + rsTarget.F05084 AS SPECIMEN_CONCEPT_ID 
		,'1791@32' AS SPECIMEN_TYPE_CONCEPT_ID
        ,TRY_CAST(rsTarget.F05175 AS DATE) AS SPECIMEN_DATE                                                                                                     /*1280*/
        ,TRY_CAST(rsTarget.F05175 AS DATETIME) AS SPECIMEN_DATETIME                                                                                      /*1280*/
		,NULL AS QUANTITY
		,NULL AS UNIT_CONCEPT_ID
		,STUFF(rsSource.F00152,4,0,'.') AS ANATOMIC_SITE_CONCEPT_ID                                                                              /*400*/
		,'1770@' + FU.F00070 AS DISEASE_STATUS_CONCEPT_ID                                                                                                                 /*1770*/
        ,rsTarget.F05084 AS SPECIMEN_SOURCE_ID                                                                                                                         /*740*/
		,NULL AS SPECIMEN_SOURCE_VALUE
		,NULL AS UNIT_SOURCE_VALUE
		,NULL AS ANATOMIC_SITE_SOURCE_VALUE
		,NULL AS DISEASE_STATUS_SOURCE_VALUE
		,HSP.F00006 AS MRN
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.DxStg rsTarget ON rsTarget.FK2 = rsSource.uk
  JOIN UNM_CNExTCases.dbo.FollowUp FU ON FU.uk = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
  WHERE F05084 NOT IN ( '00','09')