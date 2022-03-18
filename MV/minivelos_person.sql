-- Debbie's Notes: 3/10/22
-- There is an Internal Person ID (PK_Person) and MRN (PersonCode) -- OMOP Person_ID = MRN value?
-- Location_id = PK_Person (Internal Person_ID) (Inigo - let's discuss if this is ok)
-- Care_Site_id = Treatment (or Enrollment)  = hard-coded to 'UNM - CRTC'  
-- No provider because no treatment data in Velos and could have multiple PIs if on multiple studies
-- Is there only 1 test patient?
-- Don't know what to use for Modified_DtTm
-- Person_SOURCE_VALUE? Wasn't sure what to put here -- sample had last-name -- I concatenated name
SELECT 'IDENTITY_CONTEXT','SOURCE_PK','PERSON_ID','GENDER_CONCEPT_ID','YEAR_OF_BIRTH','MONTH_OF_BIRTH','DAY_OF_BIRTH','BIRTH_DATETIME','DEATH_DATETIME','RACE_CONCEPT_ID','ETHNICITY_CONCEPT_ID','LOCATION_ID','PROVIDER_ID','CARE_SITE_ID','PERSON_SOURCE_VALUE','GENDER_SOURCE_VALUE','GENDER_SOURCE_CONCEPT_ID','RACE_SOURCE_VALUE','RACE_SOURCE_CONCEPT_ID','ETHNICITY_SOURCE_VALUE','ETHNICITY_SOURCE_CONCEPT_ID','MRN','Modified_DtTm' 
UNION ALL
SELECT DISTINCT
	'MINIVELOS PERSON(OMOP_PERSON)' AS IDENTITY_CONTEXT
	,src.pkPerson_p  AS SOURCE_PK   -- internal ID
    ,src.PersonCode_p as Person_ID  -- MRN
    ,''  AS GENDER_CONCEPT_ID	 
	,SUBSTRING(src.birthDt_p,1,4) AS YEAR_OF_BIRTH	            
	,SUBSTRING(src.birthDt_p,6,2) AS MONTH_OF_BIRTH              
	,SUBSTRING(src.birthDt_p,9,2) AS DAY_OF_BIRTH	
	,DATE_FORMAT(src.BirthDt_p,'%Y-%m-%d %H:%i:%s') AS BIRTH_DATETIME	-- need to format            
	,DATE_FORMAT(src.DeathDt_p,'%Y-%m-%d %H:%i:%s') AS DEATH_DATETIME	-- need to format  
	,'' AS RACE_CONCEPT_ID	
    ,'' AS ETHNICITY_CONCEPT_ID 
    ,pkPerson_p AS LOCATION_ID	       
	,'' AS PROVIDER_ID 					
	,50 AS CARE_SITE_ID	 
    ,UPPER(concat(patLast_P,", ",patFirst_p)) as PERSON_SOURCE_VALUE
    ,GENDER_P AS GENDER_SOURCE_VALUE
    ,'' AS GENDER_SOURCE_CONCEPT_ID
	,Race_p_lu AS RACE_SOURCE_VALUE
    ,'' RACE_SOURCE_CONCEPT_ID
    ,ETHNICITY_P AS ETHNICITY_SOURCE_VALUE
    ,'' as ETHNICITY_SOURCE_CONCEPT_ID
    ,src.PersonCode_p as MRN  
	,DATE_FORMAT(curDate(),'%Y-%m-%d %H:%i:%s') As modified_DtTm 
FROM MINIVELOS.DM_PATIENT src
WHERE src.PkStudy_St is not null and src.PersonCode_P is not null
	and src.Enroll_dt_pp >= '2010-01-01'
and  (		
		src.treatmentOrg_pp_lu ='UNM - CRTC'   
    or (src.treatmentOrg_pp_lu is null and src.enrollOrg_pp_lu = 'UNM - CRTC')
    )   
and src.PERSONCODE_P <> '00001234 TestPatient'
INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_person.dat'
FIELDS TERMINATED BY '|'
ESCAPED BY "" 
LINES TERMINATED BY '\n'

