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



02/03/22 -- Fixed formatting on Modified_DtTm 
		-- Wrapped NTE.SUBJECT and PRO.TEXT in Replace statements to remove CRLF characters per KG request
02/03/22 -- Added nte.Edit_DtTm >= '2010-01-01' -- start date 
02/14/22 -- Included fields PROMPT.PGROUP (#NT1) and NTE.Note_Type (aka PRO.ENUM) because these determine what types of notes to select from MQ; not sure which OMOP fields to map to
02/14/22 -- Updated OMOP Classification List for Kevin.  Note _Type is not always set accurately because the type is selected manually by person keying in the note.  
		 -- Therefore, classification and note_types selection are "best guess".  Not sure how valuable data will be. DAH
02/14/22 -- Added Provider/Staff who created note
*/
-- drop table #notes
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|NOTE_ID|PERSON_ID|NOTE_EVENT_ID|NOTE_EVENT_FIELD_CONCEPT_ID|NOTE_DATE|NOTE_DATETIME|NOTE_TYPE_CONCEPT_ID|NOTE_CLASS_CONCEPT_ID|NOTE_TITLE|NOTE_TEXT|ENCODING_CONCEPT_ID|LANGUAGE_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|NOTE_SOURCE_VALUE|Modified_DtTm';
SELECT 'MOSAIQ NOTES(OMOP_NOTE)' AS IDENTITY_CONTEXT 
 	   ,NTE.NOTE_ID AS SOURCE_PK  
 	   ,NTE.NOTE_ID AS NOTE_ID
       ,NTE.pat_id1 AS PERSON_ID
 	   ,PRO.PGROUP  AS NOTE_EVENT_ID  -- GROUP containing Notes entered in MQ UI by providers and admin staff
	   ,NTE.Note_Type AS NOTE_EVENT_FIELD_CONCEPT_ID  -- combine with Pro.PGroup to determine which note types to report
	   ,isNULL(FORMAT(NTE.CREATE_DTTM,'yyyy-MM-dd 00:00:00'), '') AS NOTE_DATE
	   ,isNULL(FORMAT(NTE.CREATE_DTTM,'yyyy-MM-dd HH:mm:ss'), '') AS NOTE_DATETIME
	   ,isNULL(RTRIM(PRO.TEXT), '')		AS NOTE_TYPE_CONCEPT_ID
	   ,'EHR NOTES'		    AS NOTE_CLASS_CONCEPT_ID
	   ,case when NTE.SUBJECT <> ' ' 
			THEN isNULL(RTRIM( REPLACE(REPLACE(REPLACE(NTE.SUBJECT, CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
			ELSE isNULL(RTRIM( REPLACE(REPLACE(REPLACE(PRO.TEXT, CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
		END NOTE_TITLE
	   ,isNULL(REPLACE(replace(replace(RTRIM(MosaiqAdmin.dbo.RTF2TXT(NTE.NOTES)),CHAR(13),''),CHAR(10),''), '|','-' ) , '') AS NOTE_TEXT
	   ,''	AS ENCODING_CONCEPT_ID -- 'UTF-8 (32678)' AS ENCODING_CONCEPT_ID (?)
	   ,''	AS LANGUAGE_CONCEPT_ID -- '4182347' AS LANGUAGE_CONCEPT_ID (?)
	   ,isNull(NTE.Create_ID, '')  AS PROVIDER_ID  -- person who created the note; may be a provider or may be administrative staff
	   ,''  AS VISIT_OCCURRENCE_ID   -- ??
	   ,''  AS VISIT_DETAIL_ID      -- don't set
      --   ,isNULL(REPLACE(replace(replace(RTRIM(MosaiqAdmin.dbo.RTF2TXT(NTE.NOTES)),CHAR(13),''),CHAR(10),''), '|','-' ) , '')  AS NOTE_SOURCE_VALUE
           ,''   AS NOTE_SOURCE_VALUE   -- per #51 (github): too big extract, remove this dupe of NOTE_TEXT
	   ,isNULL(FORMAT(nte.Edit_DtTm,'yyyy-MM-dd HH:mm:ss'), '') as Modified_DtTm
FROM MOSAIQ.dbo.NOTES nte
LEFT JOIN Mosaiq.dbo.Prompt pro on  pro.enum = nte.note_type 
INNER JOIN MosaiqAdmin.dbo.Ref_Patients pat on nte.pat_id1 = pat.pat_id1 and is_valid = 'Y'
INNER JOIN MosaiqAdmin.dbo.RS21_Patient_list_for_Security_review subset on nte.pat_id1 = subset.pat_id1
WHERE nte.Edit_DtTm >= '2010-01-01' -- start date 
AND	nte.status_enum <> 1 -- exclude voided notes
AND  pro.pgroup = '#NT1' -- Note_ID not unique -- Use combo of Pgroup and Note_id where pgroup = #NT1 --> Notes
AND NTE.Note_Type in (12,14,25,50,101,104,105,106,107,108,109,110,112,113,114,115,116,117,118,122,170,185,192,200,201,202,207,209,210,211,212,213,15039)
;


