/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables

-- understand meta data to explore or do data profiling

-- DP = explore the data to identify column names and data types
-- source_crm go inside and go to the first table 
-- naming convention for Bronze = <sourcesystem>_<entity>
-- <x> is equivalent to a variable
===============================================================================
*/
SELECT *
FROM silver.crm_cust_info
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL /*User defined table */
	DROP TABLE silver.crm_cust_info;

GO

CREATE TABLE silver.crm_cust_info
-- defining columns, one to one
(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status  NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE,
	-- Meta Data Column
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);


-- Create SQL DDL scripts for all CSV files (6 total tables)
-- Just creates empty filelrs but does not injest the data

-- gotta check if the table is already created or not. 
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL /*User defined table */
	DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info 
(
prd_id		 INT,
cat_id		 NVARCHAR(50),
prd_key		 NVARCHAR(50),
prd_nm		 NVARCHAR(50),
prd_cost	 INT, 
prd_line	 NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt	 DATETIME,
dwh_create_date		DATETIME2 DEFAULT GETDATE()

);

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL /*User defined table */
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details 
(
sls_ord_num		NVARCHAR(50),
sls_prd_key		NVARCHAR(50),
sls_cust_id		INT,
sls_order_dt	DATE,
sls_ship_dt		DATE,
sls_due_dt		DATE,
sls_sales		INT,
sls_quantity	INT,
sls_price		INT,
dwh_create_date		DATETIME2 DEFAULT GETDATE()

);


GO
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL/*User defined table */
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('silver.erp_LOC_A101', 'U') IS NOT NULL /*User defined table */
	DROP TABLE silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101 
(
CID		NVARCHAR(50),
CNTRY	NVARCHAR(50),
dwh_create_date		DATETIME2 DEFAULT GETDATE()
);

GO
IF OBJECT_ID ('silver.erp_PX_CAT_G1V2', 'U') IS NOT NULL /*User defined table */
	DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2 
(
ID			NVARCHAR(50),
CAT			NVARCHAR(50),
SUBCAT		NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date		DATETIME2 DEFAULT GETDATE()

);

GO


