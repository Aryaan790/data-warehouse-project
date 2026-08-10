
SELECT 
cid,
bdate,
gen 
FROM bronze.erp_CUST_AZ12

-- checks
SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present; SUBSTRING(string, start point of extraction, rest of characters LEN(cid))
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			gen
		FROM bronze.erp_cust_az12
	WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

	-- Identify Out-of-Range Dates

	SELECT DISTINCT 
	bdate
	FROM bronze.erp_CUST_AZ12
	WHERE bdate < '1924-01-01' OR bdate > GETDATE()