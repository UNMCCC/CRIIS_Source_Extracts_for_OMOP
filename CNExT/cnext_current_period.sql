
/**
 We would like to see when was the last time a dataset has been 
 sourced, and pass it to LDS.
 In CNExT, we create the extract-data from the source database
 directly. Thus, if an extract is successful, all we need to 
 do is ask what date is it.
 Calling this script after we call the extraction would do.
**/
SET NOCOUNT ON;
select convert(varchar(10),Getdate(),121);