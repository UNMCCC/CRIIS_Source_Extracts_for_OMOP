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
Select 
  min(DATE(AA.rdt)) 
from
 (
  select 
   max(xDMrunDt) as rdt
   from minivelos.DM_patient_statuses -- observation
  union
  select max(xDMrunDt) as rdt
   from minivelos.dm_patient  -- proxy for location and patient.
   union
  select max(xDMrunDt) as rdt
   from minivelos.dm_Study   -- provider, study
   union
  select max(xDMrunDt) as rdt
   from minivelos.dm_patient_Enrollments -- provider, study
  )
  as AA
INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_current_period.dat'
LINES TERMINATED BY '\n'
