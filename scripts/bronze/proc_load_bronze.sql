/*
====================================================================
Stored Procedure: Loads the Data from Source into the Bronze Layer 
====================================================================

Script Purpose:
  This is a stored procedure that loads data from the source CSV files to the bronze layer tables.
  It truncates the tables  and then loads the data using the `BULK INSERT` command.

Parameters:
  None

Usage Example:
  EXEC bronze.load_bronze
 
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_global DATETIME, @end_time_global DATETIME;

	SET @start_time_global = GETDATE()

	BEGIN TRY
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		-- crm cust info
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';


		-- crm prd info
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>> Inserting Data Into: bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info 
		FROM 'D:\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';



		-- crm sales details
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>> Inserting Data Into: bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
		FROM  'D:\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';



		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';


		-- erp cust az12
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>> Inserting Into: bronze.erp_cust_az12 ';
		BULK INSERT bronze.erp_cust_az12
		FROM  'D:\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';



		-- erp loc a101
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>> Inserting Into: bronze.erp_loc_a101 ';
		BULK INSERT bronze.erp_loc_a101
		FROM  'D:\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';




		-- erp px cat g1v2
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>> Inserting Into: bronze.erp_px_cat_g1v2 ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM  'D:\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';

	SET @end_time_global = GETDATE()
	PRINT '==========================================================';
	PRINT 'Loading Bronze Layer is Completed';
	PRINT 'Total Load Duration for Bronze Layer: ' + CAST( DATEDIFF( second, @start_time_global, @end_time_global) AS NVARCHAR) + ' seconds';
	PRINT '==========================================================';

	END TRY
	BEGIN CATCH
		PRINT '==========================================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST ( ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message' + CAST ( ERROR_STATE() AS NVARCHAR)
		PRINT '==========================================================';

	END CATCH
	
END
