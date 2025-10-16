/* This script explores and performs various quality checks for the bronze layer 
before data ingestion into silver layer tables. It determines the transformations needed for various issues in the
data including the following:
- Null or duplicate primary key
- Unwanted spaces in string fields
- Invalid data ranges
- Data Consistency between related fields
*/

--================================================
--- Exploring Bronze Schema table
--================================================

-- Exploring crm_cust_info
------------------------------------------------------
SELECT *
FROM bronze.crm_cust_info;

-- Should be empty ideally
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Should return 0 rows ideally
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Consistency & Standardization
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info;

------------------------------------------------------
-- Exploring crm_prd_info
------------------------------------------------------
SELECT * 
FROM bronze.crm_prd_info


SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Null or Negetive Numbers
-- Expectations: No results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost is NULL;

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

------------------------------------------------------
-- Exploring crm_sales_details
------------------------------------------------------

-- check and explore sales_details tabel in bronze layer
SELECT
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 OR LEN(sls_order_dt) !=8

-- check if order date is less before ship and due date
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_ship_dt;

-- Check if the sales amount is not the quantity x price
SELECT DISTINCT *
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price  <=0;

SELECT DISTINCT 
sls_sales,
sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales/ NULLIF(sls_quantity,0) -- handles division by zero if any
	ELSE sls_price
END AS sls_price, 
sls_quantity
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price  <=0;


------------------------------------------------------
-- Exploring erp_cust_az12
------------------------------------------------------

-- cid in cust_az12 table should match the cst_id in crm_cust_info
SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid ,4 ,LEN(cid))
	 ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

SELECT * 
FROM silver.crm_cust_info

-- check if bdate is in valid limits
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Check the gender column
SELECT DISTINCT
gen
FROM bronze.erp_cust_az12

------------------------------------------------------
-- Exploring erp_loc_a101
------------------------------------------------------
SELECT * 
FROM bronze.erp_loc_a101

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT
REPLACE(cid,'-','') cid,
cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry)IS NULL OR cntry = ''  THEN 'n/a'
	 WHEN TRIM(cntry) IN ( 'US', 'USA') THEN 'United States'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101 

SELECT 
REPLACE(cid,'-','') cid
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN ( SELECT cst_key FROM silver.crm_cust_info)

------------------------------------------------------
-- Exploring erp_px_cat_g1v2
------------------------------------------------------
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT prd_key FROM silver.crm_prd_info;

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN ( SELECT cat_id FROM silver.crm_prd_info)