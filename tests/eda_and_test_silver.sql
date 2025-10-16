/* This script performs various quality checks for the silver layer after data ingestion.
It includes checks for:
- Null or duplicate primary key
- Unwanted spaces in string fields
- Invalid data ranges
- Data Consistency between related fields
*/

--================================================
-- Testing crm_cust_info after inserting
--------------------------------------------------

SELECT * 
FROM silver.crm_cust_info

SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Should return 0 rows ideally
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

------------------------------------------------------
-- Testing crm_prd_info
------------------------------------------------------

SELECT * 
FROM silver.crm_prd_info

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


--------------------------------------------------
-- Checking silver.crm_sales_details table
--------------------------------------------------

SELECT * 
FROM silver.crm_sales_details;

SELECT DISTINCT 
sls_sales,
sls_price, 
sls_quantity
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price  <=0;

--------------------------------------------------
-- Checking silver.erp_cust_az12
--------------------------------------------------
SELECT * 
FROM silver.erp_cust_az12;

-- check if bdate is in valid limits
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

SELECT DISTINCT gen
FROM silver.erp_cust_az12;
-----------------------------------------------------
-- checking silver.erp_loc_a101 after data ingestion
-----------------------------------------------------
SELECT * 
FROM silver.erp_loc_a101;

SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

--------------------------------------------------
-- Checking silver.erp_px_cat_g1v2 after ingestion
--------------------------------------------------
SELECT * FROM silver.erp_px_cat_g1v2