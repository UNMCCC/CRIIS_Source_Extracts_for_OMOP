/* 
Notes:
1)  First 2 attributes are included so lineage tracing and source primary key 
    identification is clearly contained in the prospective extract file. 
?
2)  The output IDENTITY_CONTEXT clearly indicates the primary source of the
    extract file.
?
3)  The output SOURCE_PK will always contain the column(s) which identify
    the primary key for the source system of record.  In cases where source 
	keys include more than one element they should be sequentially numbered,
	i.e. SOURCE_PK_01, SOURCE_PK_02,etc
?
4)  In instances where there is no equivalent concept in the source table(s),
    simply include a "NULL as MISSING_COLUMN_NAME" in the SQL so it it clear
	that the omission is not an oversight.
?
5)  The structure will largely resemble the structure of the target table and 
    wherever practical contain all source elements needed to populate the 
	target, excluding OMOP metadata keys which will be looked up during the 
	Staging to Persistent Store ETL operation.
	
6)  Single value sub-selects are recommended for external lookups vs. lots of
    joins.  Makes everything easier to read.  Additionally, all single element 
	sub selects must include TOP 1 for SQLServer or ROWNUM < 2 for Oracle instances.
	This simply protects against SQLCODE -1427 in cases where the source data 
	may not be properly constrained.
?
7)  Always include a WHERE clause even if it only validates the source
    PK is not null.
?
?
8)  Include as much commenting as needed to clearly express the work being 
    performed.
	
9)  This script is to be run and is written to be run in MS SQL Server and will generate errors if run in other DB platforms
?
*/
/* QUESTIONS/CONCERNS
 1:  Should we distinguish between Labs and Vital signs? --This is a good question and we should address during our next meeting. MM
 2:  Some that are separate in OMOP are concatenated in Mosaiq:  
		Example:  Reference Range (Total Protein) is "6.1-8.2" in MQ; OMOP captures Range_Low and Range_High
		Example:  Blood Pressure:  145/65 in MQ;	OMOP DD states that systolic and diastolic readings are recorded separately
 3:  Which field to store results
 	   -- MQ stores results as either decimal (+ or -) or string, depending upon the type of measurement.  
 4:  Which field to store field labels and/or descriptions
 5:  A provider is not associated with a Lab or Vital Sign Observation 
 6:  What OMOP fields for mapping?  Examples of how observations data may be recorded in Mosaiq:
		Float:  Temperature(%C)=36.2
		Some results that are in decimal format are returned in the String field: RBC="4.35" (as string)
		String: Protein="Negative"
		Some return values in both fields such as Body Mass Index:  Float=2.1400 and String="AutoCalcedBSA"
		Some Float fields return negative values such as Percent Weight Change=-2.6940 (DD suggests that negative values not be stored in the Value-as-number field)
		There is a Mosaiq Function that returns results (char) but rounds float values 

Notes:
 1:  May need to exclude some observations if they don't provide content (example:  Description = 'Referring & Facility'; result=
 2:  Don't know how to map data to OMOP -- RS21 -- please examine data and advise
 3:  Lab and Vital Sign labels/descriptions are probably not standard
Confidence Level MEDIUM 
Addressed NULLS 01/12/2022
02/04/2022 -- Obs_Results removed from SP -- still in this query in case KG included in the LDS
02/04/2022 -- MEASUREMENT VALUES -- OUTSTANDING TASKS
	--Values from Labs or Vitals may be a string, a float, or there may be a float value with a string qualifier/explanation
	--Not Sure how to map to OMOP Fields
02/04/2022 RANGE SETTING -- OUTSTANDING TASKS
VITALS -- Low/High values are stored in our ObsDef ref table by gender (male/female/non-gender) 
	-- In this version, I am pulling Non-Gender only.  Will need to add logic to check patient gender and then assign correct low/high values.
	-- NOTE that if there are values or female and male, then Non-gender values are set to 0.00 
	-- NEED TO PULL REFERENCE RANGE BY GENDER in source stored procedure
LABS-- The most accurate value for reference range is returned from the LAB along with Results:
	-- formatted  is a range (n-n) (EX: Range for "Total Protein" is "6.1-8.2") OR uses Greater than/Less than symbols, or may be a string value (such as NORM)
	-- If a Ref-range is not returned from the LAB, we could get the value from or ObsDef Reference table
02/04/2022 VISIT OCCURRENCE  -- OUTSTANDING TASKS
VITALS -- I CAN ADD VISIT OCCURRENCE CONCAT FIELD (patID-apptDt)  BECAUSE VITALS ARE TAKEN ON DAY PATIENT HAS APPT
LABS -- Don't know the Visit Date associated with Lab -- Labs are often ordered after a doctor appt but are for the next treatment appt.
02/04/2022 MEASUREMENT DTTM  -- Need to do some internal validation
VITALS -- Same DATE as VISIT-DATE, but Time will be the actual time that day that vitals were taken by medical staff.  May be multiple vitals per day.
LABS -- Date-TM the LAB RESULTS were entered into MOSAIQ VIA the Inbound Interface from Tricore labs, or Dt-Tm labs manually entered into the system if results not via II
02/15/2022 -- Fixed Case Statement for Range_HIGH (changed from rsource.Range_Low to corrected field of rsource.Range_high)
*/
SET NOCOUNT ON;

SELECT 'IDENTITY_CONTEXT|SOURCE_PK|MEASUREMENT_ID|PERSON_ID|MEASUREMENT_CONCEPT_ID|MEASUREMENT_DATE|MEASUREMENT_DATETIME|MEASUREMENT_TIME|MEASUREMENT_TYPE_CONCEPT_ID|OPERATOR_CONCEPT_ID|VALUE_AS_NUMBER|VALUE_AS_CONCEPT_ID|UNIT_CONCEPT_ID|RANGE_LOW|RANGE_HIGH|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|MEASUREMENT_SOURCE_VALUE|MEASUREMENT_SOURCE_CONCEPT_ID|UNIT_SOURCE_VALUE|VALUE_SOURCE_VALUE|Obs_Float|Obs_String|Observation_Bucket|Modified_DtTm';


SELECT  'MOSAIQ REF_PATIENT_MEASUREMENTS(OMOP_MEASUREMENT)' AS IDENTITY_CONTEXT
      ,rsource.obx_id	AS SOURCE_PK
      ,rsource.obx_id	AS MEASUREMENT_ID
      ,rsource.Pat_ID1	AS PERSON_ID
	  ,''				AS MEASUREMENT_CONCEPT_ID
	  ,isNULL(FORMAT(rsource.measurement_DtTm,'yyyy-MM-dd 00:00:00'),'') AS MEASUREMENT_DATE
	  ,isNULL(FORMAT(rsource.measurement_DtTm,'yyyy-MM-dd HH:mm:ss'),'') AS MEASUREMENT_DATETIME
	  ,isNULL(FORMAT(rsource.measurement_DtTm,'HH:mm:ss'),'')			 AS MEASUREMENT_TIME
	  ,'EHR observations'	AS MEASUREMENT_TYPE_CONCEPT_ID
	  ,'='			AS OPERATOR_CONCEPT_ID 
	  ,''			AS VALUE_AS_NUMBER
	  ,''			AS VALUE_AS_CONCEPT_ID 
	  ,''			AS UNIT_CONCEPT_ID   
	  ,CASE 
			WHEN Observation_Bucket = 'LAB RESULTS'	THEN isNULL(cast(rsource.lab_reference_range as char),'')	
			WHEN Observation_Bucket = 'VITAL SIGNS'	THEN isNULL(cast(rsource.Range_Low as char),'')	
			ELSE ''
		END RANGE_LOW 
	  ,CASE 
			WHEN Observation_Bucket = 'LAB RESULTS'	THEN rtrim(isNULL(cast(rsource.lab_reference_range as char),''))	
			WHEN Observation_Bucket = 'VITAL SIGNS'	THEN rtrim(isNULL(cast(rsource.Range_High as char),''))	
			ELSE ''
		END RANGE_HIGH 
	  ,''			AS PROVIDER_ID		 
	  ,''			AS VISIT_OCCURRENCE_ID  -- Vitals are taken on a day of a VISIT, but Labs may not be
	  ,''			AS VISIT_DETAIL_ID     
	  ,REPLACE(rsource.obs_desc,'|','-' )	AS MEASUREMENT_SOURCE_VALUE	--  EXAMPLE1: Temp.Tympanic (C); EXAMPLE2 :Pulse;  EXAMPLE3: Percentage of O2 Sat in Blood From Atlas the verbatim value from the source data representing the Measurement that occurred. For example, this could be an ICD10 or Read code. MM  
	  ,REPLACE(rsource.obs_label,'|','-' )			AS MEASUREMENT_SOURCE_CONCEPT_ID --- EXAMPLE1: Temperature (C);   EXAMPLE2 :P / Heart Rate; EXAMPLE3: Oxygen Saturation  
	  ,isNULL(REPLACE(rsource.obs_Units,'|','-' ),'')	AS UNIT_SOURCE_VALUE -- examples: gm/dL,secs, RATIO, %
          ,''							AS VALUE_SOURCE_VALUE --store the verbatim value that was mapped to VALUE_AS_CONCEPT_ID here MM  -- DAH: But results can be string or float or both
	  ,''							AS Obs_Results -- REMOVE THIS FIELD -- result returned from function that populates value with either the float or string value for the measurement  
	  ,isNULL(CONVERT(varchar(20),rsource.Obs_Float), '')				AS Obs_Float -- Each observation will be expressed as either a Float or a String-- if null, will set to zero which may be mistaken as a results ?
	  ,REPLACE(rsource.Obs_String,'|','-' )			AS Obs_String -- Each observation will be expressed as either a Float or a String  
	  ,rsource.Observation_Bucket				AS Observation_Bucket  -- 'LAB RESULTS' or 'VITAL SIGNS'
	  ,isNULL(FORMAT(run_date,'yyyy-MM-dd HH:mm:ss'),'') AS Modified_DtTm
  FROM MosaiqAdmin.dbo.Ref_Observation_Measurements rsource
  --INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review pat on rsource.pat_id1 = pat.pat_id1 -- subset 
  WHERE rsource.obx_id is NOT NULL
  AND rsource.measurement_DtTm >= '2010-01-01'