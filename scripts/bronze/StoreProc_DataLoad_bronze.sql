/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

Creating a store procedure:
	name convention for SP: load_<layer>
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze 

AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- SQL runs the TRY block and if it fails, it runs the CATCH block to handle the error
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT'====================================================';
		PRINT'Loading Bronze Layer';
		PRINT'====================================================';

		PRINT'----------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'----------------------------------------------------';

	SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_cust_info';
				-- Clears the table first
				TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>> Inserting Data Into: bronze.crm_cust_info';
				-- inserting data
			BULK INSERT bronze.crm_cust_info 
					--copy file path 
			FROM 'C:\Users\Aryaa\Documents\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
					-- how to handle our file with the specficiations, row header
			WITH 
			(
				FIRSTROW = 2, -- 1st row is actually the 2nd row bc that is a column aspect
				FIELDTERMINATOR = ',' , -- identifies what seperates the data
				TABLOCK -- option to improve the performance where you are locking the entire table when it is loading the data
			);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------'

				-- test the quality of your bronze table

			--	SELECT * FROM bronze.crm_cust_info 
			--	SELECT COUNT(*) FROM bronze.crm_cust_info; -- counts the data rows, cross check with the csv file

			------------------------------------------------------------
			------------------------------------------------------------
	SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_prd_info';
			TRUNCATE TABLE bronze.crm_prd_info;
		
		PRINT'>> Inserting Data Into: bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Aryaa\Documents\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH 
			(
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',' ,
				TABLOCK 
			);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------'
		--	SELECT * FROM bronze.crm_prd_info 
		--	SELECT COUNT(*) FROM bronze.crm_prd_info -- counts the data rows, cross check with the csv file

			------------------------------------------------------------
			------------------------------------------------------------
	SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details
		
		PRINT'>> Inserting Data Into: bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Aryaa\Documents\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH 
			(
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',' ,
				TABLOCK 
			);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------'
		--	SELECT * FROM bronze.crm_sales_details
		--	SELECT COUNT(*) FROM bronze.crm_sales_details -- counts the data rows, cross check with the csv file
		------------------------------------------------------------
		------------------------------------------------------------
		PRINT'----------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'----------------------------------------------------';
				
	SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_CUST_AZ12';
	
			TRUNCATE TABLE bronze.erp_CUST_AZ12;
		
		PRINT'>> Inserting Data Into: bronze.erp_CUST_AZ12';

			BULK INSERT bronze.erp_CUST_AZ12 
			FROM 'C:\Users\Aryaa\Documents\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH
			(
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',' ,
				TABLOCK 
			);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------'
		--	SELECT * FROM bronze.erp_CUST_AZ12 
		--	SELECT COUNT(*) FROM bronze.erp_CUST_AZ12 -- counts the data rows, cross check with the csv file
			------------------------------------------------------------
			------------------------------------------------------------
	SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_LOC_A101';

			TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT'>> Inserting Data Into: bronze.erp_LOC_A101';

			BULK INSERT bronze.erp_LOC_A101 
			FROM 'C:\Users\Aryaa\Documents\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH 
			(
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',' ,
				TABLOCK 
			);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------'
		--	SELECT * FROM bronze.erp_LOC_A101 
		--	SELECT COUNT(*) FROM bronze.erp_LOC_A101 -- counts the data rows, cross check with the csv file
		------------------------------------------------------------
		------------------------------------------------------------

	SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_PX_CAT_G1V2';

			TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
	
		PRINT'>> Inserting Data Into: bronze.erp_PX_CAT_G1V2';
	
			BULK INSERT bronze.erp_PX_CAT_G1V2
			FROM 'C:\Users\Aryaa\Documents\sql-ultimate-course\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH 
			(
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',' ,
				TABLOCK 
			);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------'
		--	SELECT * FROM bronze.erp_PX_CAT_G1V2 
		--	SELECT COUNT(*) FROM bronze.erp_PX_CAT_G1V2 -- counts the data rows, cross check with the csv file
	SET @batch_end_time = GETDATE();
		PRINT '===========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT'     >> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===========================================';

	END TRY
	BEGIN CATCH
		PRINT '======================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		-- cast is changing the data type, designing what can happen if during our ETL process 
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);

		PRINT '======================================='
	END CATCH

END

-- EXEC bronze.load_bronze 
