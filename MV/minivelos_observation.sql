-- is there a place for reason for Off_Study?  Such as Patient Died?  -- Or 
--
--  
--
SET @IncDate = DATE_FORMAT(DATE_ADD(curDate(),INTERVAL -3 MONTH),'%Y%m%d'  );

SELECT 'IDENTITY_CONTEXT','SOURCE_PK','OBSERVATION_ID','PERSON_ID','OBSERVATION_CONCEPT_ID','OBSERVATION_DATE','OBSERVATION_DATETIME','OBSERVATION_TYPE_CONCEPT_ID','VALUE_AS_NUMBER','VALUE_AS_CONCEPT_ID','QUALIFIER_CONCEPT_ID','UNIT_CONCEPT_ID','PROVIDER_ID','VISIT_OCCURRENCE_ID','VISIT_DETAIL_ID','OBSERVATION_SOURCE_VALUE','OBSERVATION_SOURCE_CONCEPT_ID','UNIT_SOURCE_VALUE','QUALIFIER_SOURCE_VALUE','OBSERVATION_EVENT_ID','OBS_EVENT_FIELD_CONCEPT_ID','VALUE_AS_DATETIME','Study_PK','modified_DtTm'
UNION ALL
SELECT DISTINCT
	   'MINIVELOS PATIENT_STUDY_STATUS(OMOP_OBSERVATION)' AS IDENTITY_CONTEXT  
	   ,concat_ws("-",src.PKSTUDY_ST,src.PERSONCODE_P,pkPatStudyStat_pps) AS SOURCE_PK  
       ,pkPatStudyStat_pps  AS OBSERVATION_ID 
       ,personCode_p  AS PERSON_ID 
       ,'' AS OBSERVATION_CONCEPT_ID  /*THIS IS A PLACE HOLDER AS IT MAY BE THE CORRECT VALUE*/
       ,DATE_FORMAT(statusDt_pss,'%Y-%m-%d %H:%i:%s') AS OBSERVATION_DATE 
       ,DATE_FORMAT(statusDt_pss,'%Y-%m-%d %H:%i:%s') AS OBSERVATION_DATETIME  -- NO PLACE FOR END DATE per status (most recent status will have null end date but previous will have an end date; could have gap between prev end date and new status date)
	   ,'' AS OBSERVATION_TYPE_CONCEPT_ID 
       ,'' AS VALUE_AS_NUMBER
       ,'' AS VALUE_AS_CONCEPT_ID 
       ,'' AS QUALIFIER_CONCEPT_ID 
       ,'' AS UNIT_CONCEPT_ID
	   ,ifNULL(usr.PK_User,'') as PROVIDER_ID    
       ,'' AS VISIT_OCCURRENCE_ID 
       ,'' AS VISIT_DETAIL_ID
       ,ifNULL(status_pss_lu,'') AS OBSERVATION_SOURCE_VALUE   
       ,'' AS OBSERVATION_SOURCE_CONCEPT_ID
       ,'' AS UNIT_SOURCE_VALUE
       ,'' AS QUALIFIER_SOURCE_VALUE
       ,'' AS OBSERVATION_EVENT_ID
       ,'' AS OBS_EVENT_FIELD_CONCEPT_ID     
       ,'' AS VALUE_AS_DATETIME
       ,src.PKSTUDY_ST as Study_PK    -- added 3/30/2022
       ,DATE_FORMAT(statusDt_pss,'%Y-%m-%d %H:%i:%s') As modified_DtTm
--    ,DATE_FORMAT(curDate(),'%Y-%m-%d %H:%i:%s') As modified_DtTm  
FROM MINIVELOS.DM_PATIENT_STATUSES  src
LEFT JOIN MINIVELOS.ER_USER usr on src.StudyPI_st = concat(usr.usr_lastName, ', ', usr.usr_firstName) 
     and usr.FK_CODELST_JOBTYPE = 149 -- 'Investigator' 
     and usr.FK_siteID = 50 -- 'UNM - CRTC' 
WHERE   
        DATE_FORMAT(statusDt_pss,'%Y%m%d')>= @IncDate 
        and src.PkStudy_St is not null 
	and src.PersonCode_P is not null
	and src.StudyPI_st is not null
	and src.Enroll_dt_pp >= '2010-01-01'
and  (		
		src.treatmentOrg_pp_lu ='UNM - CRTC'   
    or (src.treatmentOrg_pp_lu is null and src.enrollOrg_pp_lu = 'UNM - CRTC')
    )   
INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_observation.dat'
FIELDS TERMINATED BY '|'
ESCAPED BY "" 
LINES TERMINATED BY '\n'
