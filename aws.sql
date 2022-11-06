--AWS Athena queries

--Create table queries
--year to be updated as per requirement

CREATE TABLE IF NOT EXISTS clinicaltrialanalysis.clinicaltrial_2021
  AS
SELECT * FROM
  clinicaltrial21 -- while executing, to be updated with the table name created by the Crawler

CREATE TABLE IF NOT EXISTS clinicaltrialanalysis.mesh
  AS
SELECT * FROM
  meshbucket21 -- while executing, to be updated with the table name created by the Crawler
  
CREATE TABLE IF NOT EXISTS pharma
  AS
SELECT * FROM pharmabucket21 -- while executing, to be updated with the table name created by  the Crawler


--Problem solutions
--Question 1 
SELECT DISTINCT COUNT(*) FROM clinicaltrial_2021


--Question 2
SELECT type, COUNT(type) as Count FROM clinicaltrial_2021
GROUP BY type
ORDER BY Count DESC


--Question 3
SELECT SplitConditions, COUNT(*) AS Count FROM clinicaltrial_2021
CROSS JOIN UNNEST(SPLIT(conditions, ',')) as t(SplitConditions)
WHERE SplitConditions != ''
GROUP BY SplitConditions
ORDER BY Count DESC
LIMIT 5

--Question 4
SELECT mesh.Codes, count(mesh.Codes) as code_count
FROM (SELECT splitConditions 
      FROM clinicaltrial_2021
      CROSS JOIN UNNEST(SPLIT(conditions, ',')) as t(splitConditions)) trial
JOIN (SELECT term, SUBSTRING(tree,1,3) as Codes FROM mesh) mesh
ON trial.splitConditions = mesh.term
GROUP BY mesh.Codes
ORDER BY code_count DESC
LIMIT 10

--Question 5
SELECT trial.Sponsor, COUNT(trial.Sponsor) as sponsors_count FROM clinicaltrial_2021 trial
LEFT JOIN (SELECT DISTINCT(replace(parent_company, '"')) as unique_parent_comp FROM pharma) ph
ON trial.Sponsor = ph.unique_parent_comp
WHERE ph.unique_parent_comp IS NULL
GROUP BY trial.Sponsor
ORDER BY sponsors_count DESC
LIMIT 10

--Question 6 
CREATE VIEW Completion_Status_View AS
select date_parse(completion, '%b %Y') as Completion_Date, status 
from clinicaltrial_2021
where completion != ''

SELECT date_format(Completion_Date, '%b') as Months, COUNT(date_format(Completion_Date, '%b')) as Number_of_trials
FROM Completion_Status_View
WHERE year(Completion_Date) = 2021 AND Status = 'Completed'
GROUP BY Completion_Date
ORDER BY month(Completion_Date)
