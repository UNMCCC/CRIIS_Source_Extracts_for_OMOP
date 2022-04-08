-- Per Kevin D (RS21), 2022-04-06: Below is the attribution I would like to include in the study extract.  
-- I plan to store this as a slowly changing dimension and each time the recruitment status or phase changes we will append a row to the dimension.  
-- I correlate this will the velos observation data we can link via the “STUDY_PK” value you have in that extract – I am assuming that value is analogous to study_id below. 
-- IDENTITY_CONTEXT - VARCHAR2(64) NOT NULL   Defaulted to UNMCCC_VELOS_STUDY
-- STUDY_ID   	    -  VARCHAR2(64) NOT NULL - Unique Study Identifier without status -- DAH: 4/7/2022 STUDY_NUMBER
-- STUDY_NAME       -  VARCHAR2(256) NOT NULL Study name, short descr.
-- PROVIDER_ID      -  NUMBER NOT NULL - The provider id of the principal investigator associated with the study.
-- STUDY_TYPE       -  VARCHAR2(32) - Interventional or Observational  
	-- DAH: Field and value definitions from Rick 4/7/2022 
-- START_DATE       -  DATE NOT NULL - Date phase 0 or 1 was initiated
-- END_DATE         -  DATE NULL - Last date recruitment status moved to Suspended, Terminated, Completed, or Withdrawn.
-- RECRUITMENT_STATUS - VARCHAR2(64) NOT NULL
-- STUDY_PHASE        -  NUMBER (0 to 4)
	-- Velos stores I, I/II, I/III, II, II/III, III, III/IV, IV, V, NA, Phase IIB, Chart Review, Feasibility, Pilot 
-- STATUS_DATE        -  DATE NOT NULL - Last date the study phase or recruitment status changed.
--   DAH 4/7/2022 I ADDED STUDY_PK HERE Because I used this in concatenated keys for Observation and Observation_Period

 
SELECT 'IDENTITY_CONTEXT','STUDY_ID','STUDY_NAME','PROVIDER_ID','STUDY_TYPE','START_DATE','END_DATE','RECRUITMENT_STATUS','STUDY_PHASE','STATUS_DATE','STUDY_PK' 
UNION ALL
SELECT DISTINCT
   'MINIVELOS DM_STUDY(OMOP_STUDY)' AS IDENTITY_CONTEXT
   ,st.StudyNumber_St AS STUDY_ID	
   ,RTRIM(REPLACE(REPLACE(REPLACE(st.TITLE_ST, CHAR(13), ''), CHAR(10), ''), '|','-' )) AS STUDY_NAME  						 
   ,usr.pk_user   AS PROVIDER_ID   -- principal investigator NOTE:  Some Studies do not have a PI but have comma delimited names in another field; these have been excluded.
   ,CASE			
		WHEN st.clinicalResearchCat_stid_lu in ('INT', 'OTH INT') THEN 'Interventional'
        WHEN st.clinicalResearchCat_stid_lu in ('OBS', 'ANC/COR') THEN 'Observational'  -- No direct patient content
        ELSE ''
	END STUDY_TYPE		-- Interventional or Observational
  ,IFNULL(DATE_FORMAT(st.studyStartDt_st,'%Y-%m-%d %H:%i:%s'),'')  AS START_DATE    		
  ,IFNULL(DATE_FORMAT(st.studyEndDt_st,'%Y-%m-%d %H:%i:%s'),'')   AS END_DATE 
  ,st.orgStudyStatus_Stst_lu AS  RECRUITMENT_STATUS
  ,st.phase_st_lu AS STUDY_PHASE
  ,IFNULL(DATE_FORMAT(st.statusValidFromDt_stst,'%Y-%m-%d %H:%i:%s'),'')  AS STATUS_DATE
  ,st.pkStudy_st as STUDY_PK
FROM MINIVELOS.DM_PATIENT_ENROLLMENTS  src
INNER JOIN MINIVELOS.DM_STUDY  st on src.pkStudy_st = st.pkStudy_st
INNER JOIN MINIVELOS.ER_USER usr on st.piLast_st_lu = usr.usr_lastName and st.piFirst_st_lu = usr.usr_firstName
WHERE src.PkStudy_St is not null and src.PersonCode_P is not null
	and src.Enroll_dt_pp >= '2010-01-01'
and  (		
		src.treatmentOrg_pp_lu ='UNM - CRTC'   
    or (src.treatmentOrg_pp_lu is null and src.enrollOrg_pp_lu = 'UNM - CRTC')
    )   
and src.PERSONCODE_P <> '00001234 TestPatient'
and st.piLast_st_lu  is not null
GROUP BY st.pkStudy_st, st.statusValidFromDt_stst
INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_study.dat' FIELDS TERMINATED BY '|'
ESCAPED BY "" 
LINES TERMINATED BY '\r\n'