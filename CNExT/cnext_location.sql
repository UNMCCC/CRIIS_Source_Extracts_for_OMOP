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
LTV - 2/4/2022 - handled NULL values with the ISNULL function.

*/

SELECT  'CNEXT TUMOR(OMOP_LOCATION)' AS IDENTITY_CONTEXT /*location at Dx*/
      ,ISNULL(rsSource.FK1, '')  AS SOURCE_PK
      ,ISNULL(rsSource.FK1, '')  AS  LOCATION_ID
      ,ISNULL(rsSource.F00012, '') AS ADDRESS_1                /*2330*/
      ,'' AS ADDRESS_2                                         /*2355*/
      ,ISNULL(rsSource.F00013, '') AS CITY                     /*70*/
      ,ISNULL(rsSource.F00014, '') AS STATE                    /*80*/
      ,ISNULL(rsSource.F00015, '') AS ZIP                      /*100*/
      ,ISNULL(rsSource.F00017, '') AS COUNTY                   /*90*/
      ,ISNULL(rsSource.FK1, '')  AS LOCATION_SOURCE_VALUE
      ,'' AS LATITUDE
      ,'' AS LONGITUDE
	  ,ISNULL(HSP.F00006, '') AS MRN
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
  UNION ALL
 SELECT TOP 1000 'CNEXT PATEXTENDED(OMOP_LOCATION)' AS IDENTITY_CONTEXT /*current location*/
         ,ISNULL(rsSource.uk, '') AS SOURCE_PK
         ,ISNULL(rsSource.uk, '') AS LOCATION_ID
         ,ISNULL(rsSource.F05296, '') AS ADDRESS_1                  /*2350*/
         ,ISNULL(rsSource.F05297, '') AS ADDRESS_2                  /*2355*/
         ,ISNULL(rsSource.F05269, '') AS CITY                       /*1810*/
         ,ISNULL(rsSource.F05270, '') AS STATE                      /*1820*/
         ,ISNULL(rsSource.F05271, '') AS ZIP                        /*1830*/
         ,ISNULL(rsSource.F05272, '') AS COUNTY                     /*1840*/
         ,ISNULL(rsSource.F00004, '') AS LOCATION_SOURCE_VALUE
         ,'' AS LATITUDE
         ,'' AS LONGITUDE
		 ,ISNULL(HSP.F00006, '') AS MRN
    FROM UNM_CNExTCases.dbo.PatExtended rsSource
	JOIN UNM_CNExTCases.dbo.Tumor ON Tumor.fk1 = rsSource.UK
    JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = Tumor.uk
ORDER BY 2,3,1