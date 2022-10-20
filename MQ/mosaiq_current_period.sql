/**
 Would like to see when was the last time a dataset has been 
 sourced. We can ask two questions:
1) what is the oldest run-date for all ref-system
2) when was the last time the LDS-bound extracts ran?

the first question can be answered with the queries below.

the second question can be integrated in the first Q, by
running these queries in the same job the ETLs run.  if etls
are unnsuccessful, these wont run. if they do, the files
correspond to the dates below 
**/
SET NOCOUNT ON;
Select 
  min(convert(varchar(10),AA.rdt,121)) 
from
 (
  select 
   max(run_date) as rdt
   from MosaiqAdmin.dbo.Ref_patients
  union
  select 
   max(run_date) as rdt
   from MosaiqAdmin.dbo.Ref_schsets
  union
  select max(run_date) as rdt
   from MosaiqAdmin.dbo.Ref_SchSet_Charges
   union
  select max(run_date) as rdt
   from MosaiqAdmin.dbo.Ref_CPTs_and_Activities
   union
  select max(run_date) as rdt
   from MosaiqAdmin.dbo.Ref_ObsDefs_Assessments
   union
  Select max(run_date) as rdt 
   from MosaiqAdmin.dbo.Ref_Patient_Drugs_Administered
   union
  select max(run_date) as rdt
   from MosaiqAdmin.dbo.Ref_Patient_Diagnoses
   union
  select max(run_date) as rdt
   from MosaiqAdmin.dbo.Ref_ObsDefs_in_Buckets
   union
  select max(run_date) as rdt
    from MosaiqAdmin.dbo.Ref_Observation_Assessments
   union
  select max(run_date) as rdt
    from MosaiqAdmin.dbo.Ref_Observation_Measurements
   union
  select max(run_date) as rdt
    from MosaiqAdmin.dbo.Ref_Observations
  )
  as AA