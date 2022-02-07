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

12/13/2021 -- This was written by RS21 under the assumption that notes in Mosaiq are official Provider Clinic Visit Notes.
However, Provider Clinical Visit Notes are stored in Cerner with PDF saved in UNMCCC Shared Directories
Notes in this context refer to treatment and administrative comments recorded in Mosaiq by staff including providers, clerical, techical, and administrative staff
Type of Notes Included: (some types do not contain notes)
and (
   nte.text in ('Allergies/Alerts','Care Plan','Care Plan Activity')
or nte.text in ('Chemotherapy Tx', 'Chemo Teaching','Clinical Trial','Clinical Trial_old')
or nte.text in ('Clinical-General', 'Clinical-Past Hx', 'Clinical-Pharmacy', 'Clinical-Onc Hx', 'Clinical-Prev Rad','Clinical-RO MD no','Clinical-Fam Hx')
or nte.text in ('Code Link Component','Code Link Component','Code Link Name','CrossOvr')   -- empty
or nte.text in ('Diagnosis','Dose','Dose Action Point', 'Dose Site','Dosimetry')
or nte.text in ('eChart Check','MQ App Support notes')
or nte.text in ('Nursing','Observation','Patient Care Plan','Patient Education','Physician','Physics','Prescription', 'Protocol')
or nte.text in ('RT Plan','Screen MSG', 'Structure Set','Transfusion','Transfusion Reaction')
)
Confidence Level 60% -- are the included note types useful?  And have I included/excluded correctly

01/12/22 -- addressed NULLs
EXECUTION CHECK SUCCESSFUL DAH 01/12/2022
02/03/22 -- Fixed formatting on Modified_DtTm 
		-- Wrapped NTE.SUBJECT and PRO.TEXT in Replace statements to remove CRLF characters per KG request
02/03/22 -- Added nte.Edit_DtTm >= '2010-01-01' -- start date 
*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|NOTE_ID|PERSON_ID|NOTE_EVENT_ID|NOTE_EVENT_FIELD_CONCEPT_ID|NOTE_DATE|NOTE_DATETIME|NOTE_TYPE_CONCEPT_ID|NOTE_CLASS_CONCEPT_ID|NOTE_TITLE|NOTE_TEXT|ENCODING_CONCEPT_ID|LANGUAGE_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|NOTE_SOURCE_VALUE|Modified_DtTm';
SELECT 'MOSAIQ NOTES(OMOP_NOTE)' AS IDENTITY_CONTEXT 
 	   ,NTE.NOTE_ID AS SOURCE_PK 
 	   ,NTE.NOTE_ID AS NOTE_ID
       ,NTE.pat_id1 AS PERSON_ID
 	   ,NTE.NOTE_ID  AS NOTE_EVENT_ID
	   ,NTE.NOTE_ID  AS NOTE_EVENT_FIELD_CONCEPT_ID
	   ,isNULL(FORMAT(NTE.CREATE_DTTM,'yyyy-MM-dd 00:00:00'), '') AS NOTE_DATE
	   ,isNULL(FORMAT(NTE.CREATE_DTTM,'yyyy-MM-dd HH:mm:ss'), '') AS NOTE_DATETIME
	   ,isNULL(RTRIM(PRO.TEXT), '')		AS NOTE_TYPE_CONCEPT_ID
	   ,''							 AS NOTE_CLASS_CONCEPT_ID
	   ,case when NTE.SUBJECT <> ' ' 
			THEN isNULL(RTRIM( REPLACE(REPLACE(REPLACE(NTE.SUBJECT, CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
			ELSE isNULL(RTRIM( REPLACE(REPLACE(REPLACE(PRO.TEXT, CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
		END NOTE_TITLE
	   ,isNULL(replace(replace(RTRIM(MosaiqAdmin.dbo.RTF2TXT(NTE.NOTES)),CHAR(13),''),CHAR(10),'') , '') AS NOTE_TEXT
	   ,''	AS ENCODING_CONCEPT_ID -- 'UTF-8 (32678)' AS ENCODING_CONCEPT_ID (?)
	   ,''	AS LANGUAGE_CONCEPT_ID -- '4182347' AS LANGUAGE_CONCEPT_ID (?)
	   ,''  AS PROVIDER_ID
	   ,''  AS VISIT_OCCURRENCE_ID   -- ??
	   ,''  AS VISIT_DETAIL_ID      -- don't set
       ,'MOSAIQ.dbo.NOTES' AS NOTE_SOURCE_VALUE
	   ,isNULL(FORMAT(nte.Edit_DtTm,'yyyy-MM-dd HH:mm:ss'), '') as Modified_DtTm
FROM MOSAIQ.dbo.NOTES nte
LEFT JOIN Mosaiq.dbo.Prompt pro on  pro.enum = nte.note_type 
INNER JOIN MosaiqAdmin.dbo.Ref_Patients pat on nte.pat_id1 = pat.pat_id1 and is_valid = 'Y'
INNER JOIN MosaiqAdmin.dbo.RS21_Patient_list_for_Security_review subset on nte.pat_id1 = subset.pat_id1
WHERE nte.Edit_DtTm >= '2010-01-01' -- start date 
AND	nte.status_enum <> 1 -- exclude voided notes
AND  pro.pgroup = '#NT1' -- Note_ID not unique -- Use combo of Pgroup and Note_id where pgroup = #NT1 --> Notes
-- EXCLUDING NOTE-TYPES (some of these note types (pro.text) are not in use)
and pro.text not in ('Admin-General','Admission/Referral','Authorization', 'Batch Payments', 'BatchPay Audit')
and pro.text not in ('Bill Cfg Assign','Billing Benchmarks','Billing Config','Billing RCF','Billing RVU','Billing RVU Componen','Billing Trend Info','CDS','Claim Charge','Claim Config','Claim Config Assign','Claim Delivery Name','Collections')
and pro.text not in ('Document','DNR Note', 'EDI_Def', 'EDI_DST','EDI_Log','EDI_WS', 'Ext Lab Comp', 'Ext Lab Name', 'Facility', 'Filter Charges','Financial Class','Follow-Up','HSC-Billing')
and pro.text not in ('Image', 'Image Registration', 'IMPAC','Lab','Lab/Vitals','LabComp','LabOrder','Ledger Archive','Ledger Audit','Ledger names','Ledger transactions')
and pro.text not in ('GPC','GPC Component','HSC-FC')
and pro.text not in ('Medical Records','Medication Admin','Name/I.D.','OCI-Billing','OCI-FC','Old Ledger', 'Old Progress','Order')
and pro.text not in ('Patient Payer','Pat Statement Dtl','Pat Statement Hdr','Patient Charge', 'Patient Trial', 'People / Places')
and pro.text not in ('Print/Transmit Claim','PSDA Note','Quality Check List','Returned Mail','Rx Check/Waste','Schedule','SCP','Shape', 'Site Simulation','Trial Sponsor','XA Image')
AND nte.Edit_DtTm >= '2010-01-01' 
;

