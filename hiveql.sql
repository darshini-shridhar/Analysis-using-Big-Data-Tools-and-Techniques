-- Databricks notebook source
set hivevar:year = 2021;
set hivevar:tableName = clinicaltrial_${hivevar:year};
set hivevar:pathname="dbfs:/FileStore/tables/clinicaltrial_${hivevar:year}.csv";

-- COMMAND ----------

--Creating Clinical Trials table
DROP TABLE IF EXISTS ${hivevar:tableName};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:tableName}(
Id STRING,
Sponsor STRING,
Status STRING,
Start STRING,
Completion STRING,
Type STRING,
Submission STRING,
Conditions STRING,
Interventions STRING)
USING CSV
OPTIONS (path ${hivevar:pathname},
         delimiter "|",
         header "true");

-- COMMAND ----------

--Creating Mesh table
DROP TABLE IF EXISTS mesh;

CREATE EXTERNAL TABLE IF NOT EXISTS mesh(
term STRING,
tree STRING)
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/mesh.csv",
        delimiter ",",
        header "true");

-- COMMAND ----------

--Creating Pharma table
DROP TABLE IF EXISTS pharma;

CREATE EXTERNAL TABLE IF NOT EXISTS pharma
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/pharma.csv",
        delimiter ",",
        header "true");

-- COMMAND ----------

--1. The number of studies in the dataset. You must ensure that you explicitly check distinct studies
SELECT DISTINCT COUNT(*) FROM ${hivevar:tableName} 

-- COMMAND ----------

--2. You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. 
-- These should be ordered from most frequent to least frequent.

SELECT Type, COUNT(Type) as Count FROM ${hivevar:tableName}
GROUP BY Type
ORDER BY COUNT(Type) DESC

-- COMMAND ----------

--3. The top 5 conditions (from Conditions) with their frequencies.
SELECT Split_Conditions, COUNT(*) AS Count FROM ${hivevar:tableName}
LATERAL VIEW EXPLODE(SPLIT(Conditions, ",")) as Split_Conditions
GROUP BY Split_Conditions
ORDER BY Count DESC
LIMIT 5

-- COMMAND ----------

--4. Each condition can be mapped to one or more hierarchy codes. The client wishes to know the 5 most frequent roots (i.e. the sequence of letters and numbers before the first full stop) after this is done
SELECT mesh.Codes, count(mesh.Codes) as code_count
FROM (SELECT split_conditions 
      FROM ${hivevar:tableName}
      LATERAL VIEW EXPLODE(SPLIT(Conditions, ",")) as split_conditions) trial
JOIN (SELECT term, SUBSTRING(tree,1,3) as Codes FROM mesh) mesh
ON trial.split_conditions = mesh.term
GROUP BY mesh.Codes
ORDER BY code_count DESC
LIMIT 10

-- COMMAND ----------

--5. Find the 10 most common sponsors that are not pharmaceutical companies, along with 
--the number of clinical trials they have sponsored. Hint: For a basic implementation, 
--you can assume that the. Parent Company column contains all possible pharmaceutical companies.

SELECT trial.Sponsor, COUNT(trial.Sponsor) as sponsors_count FROM ${hivevar:tableName} trial
LEFT JOIN (SELECT DISTINCT(Parent_Company) as unique_parent_comp FROM pharma) ph
ON trial.Sponsor = ph.unique_parent_comp
WHERE ph.unique_parent_comp IS NULL
GROUP BY trial.Sponsor
ORDER BY sponsors_count DESC
LIMIT 10


-- COMMAND ----------

--6.Plot number of completed studies each month in a given year – for the submission dataset, the year is 2021. 
-- You need to include your visualization as well as a table of all the values you have plotted for each month.
-- Creating view 

DROP VIEW IF EXISTS Completion_Status_View;

CREATE VIEW Completion_Status_View AS
SELECT Status, to_date(Completion, 'MMM yyyy') as Completion_Date
FROM ${hivevar:tableName}

-- COMMAND ----------

-- #6 Cont. - Displaying the data
SELECT date_format(Completion_Date, 'MMM') as Months, COUNT(date_format(Completion_Date, 'MMM')) as Number_of_trials
FROM Completion_Status_View
WHERE year(Completion_Date) = ${hivevar:year} AND Status = 'Completed'
GROUP BY Completion_Date
ORDER BY month(Completion_Date)

-- COMMAND ----------

-- #6 #cont. Visualising the data
SELECT date_format(Completion_Date, 'MMM') as Months, COUNT(date_format(Completion_Date, 'MMM')) as Number_of_trials 
FROM Completion_Status_View
WHERE year(Completion_Date) = ${hivevar:year} AND Status = 'Completed'
GROUP BY Completion_Date
ORDER BY month(Completion_Date)
