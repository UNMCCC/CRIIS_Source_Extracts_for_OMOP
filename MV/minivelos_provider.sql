-- Debbie's notes 3/11/2022
-- orignial query pulls from PatFacility -- but I think that is the registering facility, not where treatment was provided
-- Finding all PIs on studys where patients are receiving treatment at UNM (or we think they are) -- see WHERE criteria
-- Getting user-key from er_user and Other-PIs concatenated string from DM_Study
-- Then will need to add provider to mq_observation_study (but there is no field for it)...Has RS21 given feedback on other candidtates for storing patient/study/PI data?
SELECT 'IDENTITY_CONTEXT','SOURCE_PK','PROVIDER_ID','PROVIDER_NAME',
'NPI','DEA','SPECIALTY_CONCEPT_ID','CARE_SITE_ID','YEAR_OF_BIRTH','GENDER_CONCEPT_ID','PROVIDER_SOURCE_VALUE','SPECIALTY_SOURCE_VALUE','SPECIALTY_SOURCE_CONCEPT_ID','GENDER_SOURCE_VALUE','GENDER_SOURCE_CONCEPT_ID','modified_dtTm'

UNION ALL
SELECT DISTINCT
	'MINIVELOS DM_PATIENT_ENROLLMENTS (OMOP_PROVIDER)' AS IDENTITIY_CONTEXT
        ,usr.pk_user as SOURCE_PK
        ,usr.pk_user AS PROVIDER_ID
        ,IFNULL(concat (trim(usr.usr_LastName),', ',trim(usr.usr_FirstName)),'') as PROVIDER_NAME
	,'' AS NPI
	,'' AS DEA   
        ,'' AS SPECIALTY_CONCEPT_ID
        ,'' AS CARE_SITE_ID
        ,'' AS YEAR_OF_BIRTH
	,'' AS GENDER_CONCEPT_ID
        ,'' AS PROVIDER_SOURCE_VALUE
        ,'' AS SPECIALTY_SOURCE_VALUE 
	,'' AS SPECIALTY_SOURCE_CONCEPT_ID 
	,'' AS GENDER_SOURCE_VALUE /* Gender of the provider is not captured in MiniVeloS*/
	,'' AS GENDER_SOURCE_CONCEPT_ID /* Gender of the provider is not captured in Velos*/
        ,DATE_FORMAT(curDate(),'%Y-%m-%d %H:%i:%s') As modified_DtTm 
FROM MINIVELOS.DM_PATIENT_ENROLLMENTS  src
INNER JOIN MINIVELOS.dm_Study st on src.pkStudy_st = st.pkStudy_st
INNER JOIN MINIVELOS.ER_USER usr on st.piLast_st_lu = usr.usr_lastName and st.piFirst_st_lu = usr.usr_firstName
WHERE src.PkStudy_St is not null 
and src.PersonCode_P is not null
	and src.Enroll_dt_pp >= '2010-01-01'
and  (		
		src.treatmentOrg_pp_lu ='UNM - CRTC'   
    or (src.treatmentOrg_pp_lu is null and src.enrollOrg_pp_lu = 'UNM - CRTC')
    )   
and src.PERSONCODE_P <> '00001234 TestPatient'
and usr.pk_user > 0 
INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_provider.dat'
FIELDS TERMINATED BY '|'
ESCAPED BY "" 
LINES TERMINATED BY '\n'


