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
)t WHERE flag_last != 1;

