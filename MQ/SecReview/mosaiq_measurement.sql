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

Execution Check 01/10/2022-- Successful DAH
*/
SET NOCOUNT ON;
SELECT "IDENTITY_CONTEXT|SOURCE_PK|MEASUREMENT_ID|PERSON_ID|MEASUREMENT_CONCEPT_ID|MEASUREMENT_DATE|MEASUREMENT_DATETIME|MEASUREMENT_TIME|MEASUREMENT_TYPE_CONCEPT_ID|OPERATOR_CONCEPT_ID|VALUE_AS_NUMBER|VALUE_AS_CONCEPT_ID|UNIT_CONCEPT_ID|RANGE_LOW|RANGE_HIGH|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|UNIT_SOURCE_VALUE|VALUE_SOURCE_VALUE|MEASUREMENT_SOURCE_VALUE|MEASUREMENT_SOURCE_CONCEPT_ID|Obs_Results|Obs_Float|Obs_String|Observation_Bucket";
SELECT DISTINCT 'MOSAIQ REF_PATIENT_MEASUREMENTS(OMOP_MEASUREMENT)' AS IDENTITY_CONTEXT
      ,rsource.obx_id AS SOURCE_PK
      ,rsource.obx_id AS MEASUREMENT_ID
      ,rsource.Pat_ID1 AS PERSON_ID
	  ,NULL AS MEASUREMENT_CONCEPT_ID
	  ,FORMAT(rsource.Obs_DtTm,'yyyy-MM-dd 00:00:00') AS MEASUREMENT_DATE
	  ,CASE 
			WHEN rsource.Observation_Bucket = 'Lab'			THEN FORMAT(rsource.Lab_Results_Populated_in_MQ_DtTm, 'yyyy-MM-dd HH:mm:ss')  
			WHEN rsource.Observation_Bucket = 'Vital Sign'	THEN FORMAT(rsource.Vital_Signs_Taken_DtTm, 'yyyy-MM-dd HH:mm:ss') 
	   END AS MEASUREMENT_DATETIME
	  ,CASE 
			WHEN rsource.Observation_Bucket = 'Lab'			THEN FORMAT(rsource.Lab_Results_Populated_in_MQ_DtTm ,'HH:mm:ss')  
			WHEN rsource.Observation_Bucket = 'Vital Sign'	THEN FORMAT(rsource.Vital_Signs_Taken_DtTm,'HH:mm:ss')  
			ELSE NULL
	   END AS MEASUREMENT_TIME
	  ,'EHR observations' AS MEASUREMENT_TYPE_CONCEPT_ID
	  ,'=' AS OPERATOR_CONCEPT_ID 
	  ,0 AS VALUE_AS_NUMBER
	  ,0 AS VALUE_AS_CONCEPT_ID 
	  ,rsource.obs_Units AS UNIT_CONCEPT_ID   -- examples: gm/dL,secs, RATIO, %, 
	  ,rsource.reference_range AS RANGE_LOW   -- stored in a combined string:  Range for "Total Protein" is "6.1-8.2"
	  ,rsource.reference_range AS RANGE_HIGH  -- stored in a combined string:  Range for "Total Protein" is "6.1-8.2"
	  ,NULL AS PROVIDER_ID		 
	  ,ApptDt_PatID AS VISIT_OCCURRENCE_ID  -- Populate? Only if we can map to a key value in VISIT_OCCURRENCE MM -- since we are setting visit_occurrence to the 1st appt of day -- we can map
								-- starting to wonder if visit Occurrence is set in a way that will be meaningful...hmmm.  
	  ,0 AS VISIT_DETAIL_ID      -- Only if we can map to a key value in VISIT_DETAIL MM  -- can't map to detail -- labs and vitals are not appt specific if there are multiple appts in a day
	  ,0 AS UNIT_SOURCE_VALUE
      ,0 AS VALUE_SOURCE_VALUE --store the verbatim value that was mapped to VALUE_AS_CONCEPT_ID here MM  
	  ,rsource.obs_desc  AS MEASUREMENT_SOURCE_VALUE		--  EXAMPLE1: Temp.Tympanic (C); EXAMPLE2 :Pulse;          EXAMPLE3: Percentage of O2 Sat in Blood From Atlas the verbatim value from the source data representing the Measurement that occurred. For example, this could be an ICD10 or Read code. MM  
	  ,rsource.obs_label AS MEASUREMENT_SOURCE_CONCEPT_ID	--- EXAMPLE1: Temperature (C);   EXAMPLE2 :P / Heart Rate; EXAMPLE3: Oxygen Saturation  
      ,rsource.Obs_Results  -- result returned from function that populates value with either the float or string value for the measurement  -- REMOVE THIS FIELD
	  ,rsource.Obs_Float	-- Each observation will be expressed as either a Float or a String-- in 
	  ,rsource.Obs_String	-- Each observation will be expressed as either a Float or a String  
	  ,rsource.Observation_Bucket
  FROM MosaiqAdmin.dbo.Ref_Patient_Measurements rsource
  INNER JOIN MosaiqAdmin.dbo.RS21_Patient_list_for_Security_review subset on rsource.pat_id1 = subset.pat_id1