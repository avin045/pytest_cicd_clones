select 
SPLIT(TRIM(split(EMPLOYEE_NAME,',')[1]::varchar),' ')[0]::text as FIRSTNAME,TRIM(split(EMPLOYEE_NAME,',')[0]::varchar) as LASTNAME,
-- EMPLOYEE_NAME,
EMPID,SALARY,POSITION,STATE,ZIP,DOB,SEX,DATEOFHIRE,DATEOFTERMINATION,EMPLOYMENTSTATUS,DEPARTMENT,MANAGERNAME,RECRUITMENTSOURCE,PERFORMANCESCORE,LASTPERFORMANCEREVIEW_DATE 
from hr_schema.employee_copy_tbl;