-- Inigo's notes:
-- We may need metadata about the study: Name, Principal Investigator, god forbid protocol
-- Add ENROLL END Date constraint ( >2010 )
-- Is the enroll_dt_pp for "active" in the study, or any status in relation w study (status is enrolled)
-- GROUP BY PERSONCODE_P
--
-- Debbie's Notes 3/10/22
-- MINIVELOS.DM_PATIENT_ENROLLMENTS contains 1 row per Study-ID per MRN
-- Does MINIVELOS.DM_PATIENT_ENROLLMENTS  have pkPerson_p?  Ask Rick? AND was this DM created from DM_Patient?
-- Title includes Study ID; may also include the phrase (STUDY COMPLETE) or (STUDY CLOSED) or other study status indicator such as "Closed to IRB"
-- Title = {<Study-status}: Study ID: Study Title}
-- PersonCode = MRN
-- Selecting for patients 
-- not sure what to use for Modified Dt --  Perhaps the modDt_pss (Patient Study Status Modified Dt)  which indicates a status changes?  is this ever null? 
-- Observation_period_Start_Dt:  Using Enrollment date
-- Observation_period_End_Dt: Using Patient-Study_Status_Dt from DM_PATIENT_ENROLLMENT (StatusDt_pss) when patient is "OFF STUDY"; 
	-- The date in DM_PATIENT_ENROLLMENT SHOULD RETURN MOST RECENT STATUS DATE (need to confrm by looking at SP)
 	-- NOTE THAT A PATIENT CAN GO BACK "OFF STUDY" after being marked as "OFF STUDY"
    -- Tried this but it timed out; ,(select statusDt_pss  From Minivelos.DM_Patient where DM_Patient.PersonCode_p = src.PersonCode_P and DM_Patient.studyNumber_st = src.studyNumber_st order by statusDt_pss desc Limit 1) AS OBSERVATION_PERIOD_END_DATE      
-- contraint for UNMCCC patients: need to confirm that this logic identifies UNM Health System patients; 
	-- if it does, note that patients may NOT have been seen at UNMCCC (ex: May have been UH or Pediatric Patients) 
 -- REMOVED FIELD BECAUSE NOT IN OMOP DD:  STUDYNUMBER_ST AS OBSERVATION_PERIOD_VALUE   
-- 03/30/2022 Added Distinct (source DM has multiple rows due to data we are not collecting here) DAH
-- 03/30/2022 Added code to remove carriage returns and '|' from title  DAH
-- 3/30/2022 added -- ,src.PKSTUDY_ST as Study_PK    
SELECT 'IDENTITY_CONTEXT','SOURCE_PK'	,'OBSERVATION_PERIOD_ID','PERSON_ID','OBSERVATION_PERIOD_START_DATE','OBSERVATION_PERIOD_END_DATE','PERIOD_TYPE_CONCEPT_ID','Study_PK','modified_DtTm' 
UNION ALL
SELECT DISTINCT
   'MINIVELOS DM_PATIENT_ENROLLMENTS(OMOP_OBSERVATION_PERIOD)' AS IDENTITY_CONTEXT
   ,concat_ws("-",src.PKSTUDY_ST,src.PERSONCODE_P)  AS SOURCE_PK    -- Correct PK, but we need specific to record. Concat PKStudy_St,PersonCode_p  -- should be unique								
   ,RTRIM(REPLACE(REPLACE(REPLACE(src.TITLE_ST, CHAR(13), ''), CHAR(10), ''), '|','-' ))      AS OBSERVATION_PERIOD_ID 						 
   ,src.PERSONCODE_P AS PERSON_ID      -- MRN                       
   ,DATE_FORMAT(src.ENROLL_DT_PP,'%Y-%m-%d %H:%i:%s')  AS OBSERVATION_PERIOD_START_DATE    		
   ,CASE						
	WHEN src.status_pss_lu = 'Off Study'   
	THEN DATE_FORMAT(src.statusDt_pss,'%Y-%m-%d %H:%i:%s') 	
	ELSE ''
   END OBSERVATION_PERIOD_END_DATE 
   ,'surrogate for CTMS EVELOS' AS PERIOD_TYPE_CONCEPT_ID -- Questionable, but consistent with other extracts
   ,src.PKSTUDY_ST as Study_PK    -- added 3/30/2022
   ,DATE_FORMAT(src.statusDt_pss,'%Y-%m-%d %H:%i:%s') As modified_DtTm
  
FROM MINIVELOS.DM_PATIENT_ENROLLMENTS  src
WHERE src.PkStudy_St is not null and src.PersonCode_P is not null
	and src.Enroll_dt_pp >= '2010-01-01'
and  (		
		src.treatmentOrg_pp_lu ='UNM - CRTC'   
    or (src.treatmentOrg_pp_lu is null and src.enrollOrg_pp_lu = 'UNM - CRTC')
    )   
and src.PERSONCODE_P <> '00001234 TestPatient'
INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_observation_period.dat' FIELDS TERMINATED BY '|'
ESCAPED BY "" 
LINES TERMINATED BY '\r\n'