-- Check For Nulls or Duplicates in Primary Key 
-- Expectation: No Result

-- before injesting do a quality check, aggregate the primary key
Select 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL -- added that bc if there is only one null

