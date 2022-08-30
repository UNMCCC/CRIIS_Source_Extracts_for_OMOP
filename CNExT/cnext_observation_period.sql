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

LTV - 1/31/2022 - adding patient's MRN at the end of the query per Mark. MRN is also listed as the PERSON_ID in this query.

LTV - 2/4/2022 - handled NULL values with the ISNULL function. I am removing the added MRN field because the PERSON_ID field is the
                 MRN in this case.

*/
SET NOCOUNT ON;
DECLARE @IncDate VARCHAR(8);
SET @IncDate = CONVERT(VARCHAR(8),DateAdd(week, -5, GETDATE()),112);
DECLARE @AllDates VARCHAR(8);
SET @AllDates = '20100101';
DECLARE @fromDate VARCHAR(8);
SET @fromDate = 
   CASE $(isInc)
     WHEN 'Y' THEN  @IncDate
     WHEN 'N' THEN  @AllDates
   END
-- TEST this param
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|OBSERVATION_PERIOD_ID|PERSON_ID|OBSERVATION_PERIOD_START_DATE|OBSERVATION_PERIOD_END_DATE|PERIOD_TYPE_CONCEPT_ID|MRN|Modified_DtTm';
SELECT  'CNEXT HOSPITAL(OMOP_OBSERVATION_PERIOD)' AS IDENTITY_CONTEXT
      ,rsSource.UK  AS SOURCE_PK
	  ,rsSource.UK  AS OBSERVATION_PERIOD_ID
      ,PAT.UK AS PERSON_ID                                                                                                                       /*10*/
	  ,case
		 when F00024 = '00000000'
		 then ''
		 when right(F00024, 4) = '9999'
		 then ISNULL(FORMAT(TRY_CAST(left(F00024,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 when right(F00024,2) = '99'
         then ISNULL(FORMAT(TRY_CAST(left(F00024,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 else ISNULL(FORMAT(TRY_CAST(F00024 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
	   END AS OBSERVATION_PERIOD_START_DATE 
	   ,case
	     when F00068 = '00000000'
		 then ''
		 when right(F00068, 4) = '9999'
		 then ISNULL(FORMAT(TRY_CAST(left(F00068,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 when right(F00068,2) = '99'
         then ISNULL(FORMAT(TRY_CAST(left(F00068,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 else ISNULL(FORMAT(TRY_CAST(F00068 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
	   END AS OBSERVATION_PERIOD_END_DATE
	  ,'1791@32' AS PERIOD_TYPE_CONCEPT_ID
	  ,ISNULL(rsSource.F00006, '') MRN
	  ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
        then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	    else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') end 
		as modified_dttm
  FROM UNM_CNExTCases.dbo.Hospital rsSource
  JOIN UNM_CNExTCases.dbo.Tumor TUM on TUM.uk = rsSource.fk2
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = TUM.fk1
 INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on rsSource.uk=HExt.UK
  WHERE
     rsSource.F00006 not in (999999998, 9999998, 999999, 9999)
     and rsSource.F00006 >= 1000
     and HExt.F00084 >= @fromDate
