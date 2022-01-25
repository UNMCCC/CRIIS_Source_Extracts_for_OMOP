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

*/

SELECT  'CNEXT PATIENT(OMOP_PERSON)' AS IDENTITY_CONTEXT
      ,rsSource.uk AS SOURCE_PK
      ,PatExtended.F00004 AS PERSON_ID                                       /*10*/
      ,rsSource.F00022 AS GENDER_CONCEPT_ID                                  /*220*/
      ,SUBSTRING (rsSource.F00019,1,4) AS YEAR_OF_BIRTH                      /*240*/
      ,SUBSTRING (rsSource.F00019,5,2) AS MONTH_OF_BIRTH                     /*240*/
      ,SUBSTRING (rsSource.F00019,7,2) AS DAY_OF_BIRTH                       /*240*/
	  ,TRY_CONVERT(DATETIME, rsSource.F00019, 102) AS BIRTH_DATETIME         /*240*/
      ,CASE WHEN rsSource.F00069 = 0 THEN rsSource.F00068                    /*1760*/
	        ELSE '' END AS DEATH_DATETIME                                    /*1750*/
      ,PatExtended.F00021 AS RACE_CONCEPT_ID                                 /*160*/
      ,PatExtended.F00138 AS ETHNICITY_CONCEPT_ID                            /*190*/
      ,PatExtended.F05271 AS LOCATION_ID                                     /*1830*/
	  ,HospExtended.F00675 AS PROVIDER_ID                                    /*2460*/
      ,PatExtended.F00003 AS CARE_SITE_ID                                    /*21*/
	  ,PatExtended.F00004 AS PERSON_SOURCE_VALUE                             /*20*/
      ,rsSource.F00022 AS GENDER_SOURCE_VALUE                                /*220*/
      ,'220@' + rsSource.F00022 AS GENDER_SOURCE_CONCEPT_ID                  /*220*/
	  ,PatExtended.F00021 AS RACE_SOURCE_VALUE                               /*160*/
	  ,'160@' + PatExtended.F00021 AS RACE_SOURCE_CONCEPT_ID
	  ,PatExtended.F00138 AS ETHNICITY_SOURCE_VALUE                          /*190*/
	  ,'190@' + PatExtended.F00138 AS ETHNICITY_SOURCE_CONCEPT_ID
     
  FROM UNM_CNExTCases.dbo.Patient rsSource
  JOIN UNM_CNExTCases.dbo.PatExtended on PatExtended.uk = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Tumor on rsSource.uk = Tumor.fk1
  JOIN UNM_CNExTCases.dbo.Hospital on Tumor.uk = Hospital.fk2
  JOIN UNM_CNExTCases.dbo.HospExtended ON HospExtended.uk = Hospital.uk