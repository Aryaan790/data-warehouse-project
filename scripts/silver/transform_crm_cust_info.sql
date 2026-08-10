-- data transformation and data cleansing

--my way did not show the null for some reason
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29449 OR cst_id = 29473 OR cst_id = 29433 OR cst_id = NULL OR  cst_id = 29483 OR cst_id = 29466;

-- his way (sub-table) showcased the null as we showed where the row number has the same cst_id and count it
SELECT 
*
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last != 1;

-- check for unwanted spaces
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
-- not equal to the first name after trim removes leading and trailing spaces from a string 
WHERE cst_firstname != TRIM(cst_firstname) 

SELECT 
cst_id,
cst_key, 
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname, 
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;


-- Data Standardization & Consistency for gender
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT 
cst_id,
cst_key, 
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname, 
cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
	 WHEN UPPER(TRIM(cst_gndr)) =  'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

-- ======================================================
-- Data Standardization & Consistency for marriage status
-- ======================================================

SELECT DISTINCT cst_marital_status 
FROM bronze.crm_cust_info;


-- once done with the code below then you write the insert statement.

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr, 
	cst_create_date
)
SELECT 
	cst_id,
	cst_key, 
	TRIM(cst_firstname) as cst_firstname, -- TRIMMING normalization
	TRIM(cst_lastname) as cst_lastname, 

	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
		 WHEN UPPER(TRIM(cst_marital_status)) =  'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status, -- Normalize maritalize status values tp readable format

	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
		 WHEN UPPER(TRIM(cst_gndr)) =  'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gndr, -- Normalize gender values to readable format

	cst_create_date 

FROM (
	SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL -- Removes duplicates
)t
WHERE flag_last = 1; -- data filtering (Select the most recent record per customer)

