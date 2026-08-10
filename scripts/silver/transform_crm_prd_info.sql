-- Clean & Load of production information in CRM

SELECT *

FROM bronze.crm_prd_info-- Quality Check after query 7


-- Check for Nulls or Dulpicates in Primary Key
-- Expectation: No Result 

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted Space
-- Expectation: No Results

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT 
*
FROM silver.crm_cust_info

-- check the unique category ids

SELECT Distinct id
FROM bronze.erp_PX_CAT_G1V2
-- the problem is, in the erp we have '_' and in CRM we have '-'
-- so must transform it to either or



-- So what we are doing is going down the column list one by one through the check
-- and making changes to the extraction of code as we get to it.  for the code below

-- Chek for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- CHECK for NULLS or Negative Numbers for quality of numbers
-- Expecation: No results
 SELECT prd_cost
 FROM bronze.crm_prd_info
 WHERE prd_cost < 0 OR prd_cost IS NULL

 -- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info
-- since i do not know what the values means, will have to go ask to the people what it means

-- Check for Invalid Date Orders
-- End date must not be earlier than the start date
SELECT *
FROM Bronze.crm_prd_info 
WHERE prd_end_dt < prd_start_dt
-- To handle this, narrow it to a few examples in excel and figure out what is going on,
--		Solution #1: 
--				- can swap it because the dates are not making sense. they overlap. so it is not good 
--			the end o the first history must be younger than the start second record
--		Solution #2:
--				- End date =  Start date of the 'NEXT' Record -1 
--				- FOcus on one solution in a different query, 9.1 Bronze_prd Invalid Date check

SELECT
			prd_id,
			prd_key,
			SUBSTRING(prd_key, 1, 5) AS cat_ID, -- what column, the position from where to extract, and how many characters
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

FROM bronze.crm_prd_info

/*SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
 */ 

 SELECT
		prd_id,
		prd_key,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_ID, --extract category ID 
		-- Replace(what, what to replace, what to replace with), FILters out unmatch data after applying transformatio
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- extract product key 
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
		END AS prd_line, -- Map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) as prd_start_dt,  -- data type casting 
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- data enrichment / data casting 

FROM bronze.crm_prd_info


WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
	(SELECT sls_prd_key FROM bronze.crm_sales_details) -- just products that do not have orders
	
-- check  WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT Distinct id FROM bronze.erp_PX_CAT_G1V2)
		-- any category id that is not available in this table

