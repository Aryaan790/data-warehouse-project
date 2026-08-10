-- Quality Check after query 7


-- Check for Nulls or Dulpicates in Primary Key
-- Expectation: No Result 

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

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


