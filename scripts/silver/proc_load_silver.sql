/*
==================================================================================================
Stored Procedure: Takes the Data from Bronze layer, cleans it and inserts it into the Silver Layer 
==================================================================================================

Script Purpose:
  This is a stored procedure that gets data from the tables in the bronze layer and performs necessary transformations to clean the data.
  It then loads the cleaned data into the corresponding silver layer tables.

Parameters:
  None

Usage Example:
  EXEC silver.load_silver
 
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_global DATETIME, @end_time_global DATETIME;

	SET @start_time_global = GETDATE()

	BEGIN TRY

		PRINT '==========================================';
		PRINT 'Starting Data Ingestion into Silver Layer';
		PRINT '==========================================';

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
			 ELSE 'n/a'
		END cst_marital_status,

		CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'FEMALE'
			WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'MALE'
			ELSE 'n/a'
		END cst_gender,
		cst_create_date

		FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL) t 
		WHERE flag_last = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';

		-- Cleaning and inserting data into silver.crm_prd_info
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id, -- Extracting category id
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Extracting product key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN  'M' THEN 'MOUNTAIN'
			 WHEN  'R' THEN 'ROAD'
			 WHEN  'S' THEN 'OTHER' 
			 WHEN  'T' THEN 'TOURING'
			 ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';


		-- Cleaning and Loading data into the crm_sales_details table
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- Changing the values from int to date
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- Changing the values from int to date
		END AS sls_ship_dt,

		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- Changing the values from int to date
		END AS sls_due_dt,

		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales/ NULLIF(sls_quantity,0) -- handles division by zero if any
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';

		-- Clean and Load data erp_cust_az12 from bronze to silver layer
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_cust_az12 ;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid ,4 ,LEN(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN  ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN  ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';

		-- cleaning data and inserting it into silver.erp_loc_a101
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101( cid,cntry )
		SELECT
		REPLACE(cid,'-','') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry)IS NULL OR cntry = ''  THEN 'n/a'
			 WHEN TRIM(cntry) IN ( 'US', 'USA') THEN 'United States'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';

		-- Cleaning and inserting data from erp_px_cat_g1v2 from bronze to silver
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------';

		SET @end_time_global = GETDATE()
		PRINT '==========================================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT 'Total Load Duration for Silver Layer: ' + CAST( DATEDIFF( second, @start_time_global, @end_time_global) AS NVARCHAR) + ' seconds';
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






