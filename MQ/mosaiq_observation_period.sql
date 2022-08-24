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

*/
 
/* CONFIDENCE LEVEL 75% -- is this the correct interpretation?
EXECUTION CHECK SUCCESSFUL -- DAH 01/20/2022
Added Modified DtTm -- DAH 01/20/2022
modded mod-dttm on Aug 2022
added param to pull full or incremental
*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|OBSERVATION_PERIOD_ID|PERSON_ID|OBSERVATION_PERIOD_START_DATE|OBSERVATION_PERIOD_END_DATE|PERIOD_TYPE_CONCEPT_ID|Duration_HrMin|Activity|modified_DtTm';
SELECT  'MosaiqAdmin Ref_SchSets (OMOP_OBSERVATION_PERIOD)' AS IDENTITY_CONTEXT
      ,rsource.sch_Set_Id	AS SOURCE_PK
      ,rsource.sch_Set_Id	AS OBSERVATION_PERIOD_ID
      ,rsource.Pat_ID1		AS PERSON_ID
      ,Format(rsource.Appt_DtTm,'yyyy-MM-dd HH:mm:ss')  AS OBSERVATION_PERIOD_START_DATE
	  ,''		    AS OBSERVATION_PERIOD_END_DATE
	  ,'EHR note' AS PERIOD_TYPE_CONCEPT_ID
      ,isNULL(rsource.Duration_HrMin,'') as Duration_HrMin
      ,isNULL(rsource.activity_desc,'') as Activity
	  ,isNULL(Format(rsource.schSet_create_dtTm,'yyyy-MM-dd HH:mm:ss'),'') AS modified_DtTm
  FROM MosaiqAdmin.dbo.$(varSrcTable) rsource
 -- INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review pat on rsource.pat_id1 = pat.pat_id1 -- subset 
  WHERE sch_set_id is not null
  ;
