INSERT OVERWRITE TABLE salpart
PARTITION (jobtitle)
SELECT name, agencyid, agency, hiredate, annualsalary, grosspay, jobtitle
FROM salaries;

