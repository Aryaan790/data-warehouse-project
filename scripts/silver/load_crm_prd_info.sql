TRUNCATE TABLE silver.crm_prd_info

INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	Prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
 SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Replace(what, what to replace, what to replace with), FILters out unmatch data after applying transformatio
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
	END AS prd_line, -- Map product line codes to descriptive values
	CAST(prd_start_dt AS DATE) as prd_start_dt, 
	CAST(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
		AS DATE
	) AS prd_end_dt -- Calculate end date as one day before the next start date

FROM bronze.crm_prd_info

-- >>>>>>>>>>>>>>>>>>>>

-- AFTER inserting data into table we do data checks for each column in the table

-- >>>>>>>>>>>>>>>>>>>>>>>

-- Check for Nulls or Dulpicates in Primary Key
-- Expectation: No Result 

SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted Space
-- Expectation: No Results

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check the unique category ids

SELECT Distinct prd_id
FROM silver.crm_prd_info
-- the problem is, in the erp we have '_' and in CRM we have '-'
-- so must transform it to either or



-- So what we are doing is going down the column list one by one through the check
-- and making changes to the extraction of code as we get to it.  for the code below

-- Chek for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- CHECK for NULLS or Negative Numbers for quality of numbers
-- Expecation: No results
 SELECT prd_cost
 FROM silver.crm_prd_info
 WHERE prd_cost < 0 OR prd_cost IS NULL

 -- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
-- since i do not know what the values means, will have to go ask to the people what it means

-- Check for Invalid Date Orders
-- End date must not be earlier than the start date
SELECT *
FROM silver.crm_prd_info 
WHERE prd_end_dt < prd_start_dt


-- full table check
SELECT 
*
FROM silver.crm_prd_info

Select *
FROM silver.crm_cust_info
