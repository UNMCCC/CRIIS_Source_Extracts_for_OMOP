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
LTV - 2/22/2022 - Changed the UNION ALL to only UNION to reduce the number of duplicated addresses. This reduced the number of rows returned substantially - from 128,481 to 122,157;
                  however, some addresses still appear to be duplicated but because other selected fields are different, like County, the rows are considered to be different.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|LOCATION_ID|ADDRESS_1|ADDRESS_2|CITY|STATE|ZIP|COUNTY|LOCATION_SOURCE_VALUE|LATITUDE|LONGITUDE|Modified_DTTm';
SELECT  'CNEXT TUMOR(OMOP_LOCATION)' AS IDENTITY_CONTEXT /*location at Dx*/
      ,rsSource.UK AS SOURCE_PK
      ,rsSource.UK AS LOCATION_ID
      ,ISNULL(rsSource.F00012,'') AS ADDRESS_1               /*2330*/
      ,'' AS ADDRESS_2                                         /*2355*/
      ,rsSource.F00013 AS CITY                     /*70*/
      ,rsSource.F00014 AS STATE                    /*80*/
      ,ISNULL(rsSource.F00015, '') AS ZIP                      /*100*/
      ,ISNULL(rsSource.F00017, '') AS COUNTY                   /*90*/
      ,rsSource.FK1 AS LOCATION_SOURCE_VALUE
      ,'' AS LATITUDE
      ,'' AS LONGITUDE
     ,format(GETDATE(),'yyyy-MM-dd HH:mm:ss') as Modified_DtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource

  