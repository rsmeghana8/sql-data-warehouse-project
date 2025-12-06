SELECT * 
FROM silver.crm_cust_info
----------------------------------------------------------------------------------------
-- Combing customer tables in crm and erp - Step1
----------------------------------------------------------------------------------------
SELECT cst_id , COUNT(*)
FROM 
	(SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gender,
	ci.cst_create_date,
	ca.bdate, 
	ca.gen,
	la.cntry
	FROM silver.crm_cust_info ci -- Master Table
	LEFT JOIN silver.erp_cust_az12 ca -- Use Left join to not lose data from master table
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid) t

GROUP BY cst_id
HAVING COUNT(*) > 1; -- checking for any duplicates introduced during join
--------------------------------------------------------------------------
-- We have 2 gender columns from 2 tables, lets combine them Step 2
--------------------------------------------------------------------------
SELECT DISTINCT
ci.cst_gender,
ca.gen,
CASE WHEN ci.cst_gender = 'n/a' AND ca.gen IS NOT NULL THEN ca.gen -- Priority to data in crm table and only use ca.gen data if it is n/a and not null
	 ELSE ci.cst_gender -- if null in ca gen use what ever is in crm cst_gen table
END AS new
FROM silver.crm_cust_info ci -- Master Table
LEFT JOIN silver.erp_cust_az12 ca -- Use Left join to not lose data from master table
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1,2;  

-------------------------------------------------------------------------------------
-- Combing 1 and 2 into single query and change column names to user friendly names
-- Also change the order of cols
-- Adding row number to use as primary key
-- Finally creating a VIEW
-------------------------------------------------------------------------------------
    
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER ( ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gender = 'n/a' AND ca.gen IS NOT NULL THEN ca.gen -- Priority to data in crm table and only use ca.gen data if it is n/a and not null
			ELSE ci.cst_gender -- if null in ca gen use what ever is in crm cst_gen table
	END AS gender,
	ca.bdate AS birthdate, 
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci -- Master Table
LEFT JOIN silver.erp_cust_az12 ca -- Use Left join to not lose data from master table
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

----------------------------------------------------------------------------------------
-- Joining 2 product tables considering crm_prd_info as master and checking for any 
-- duplicates
----------------------------------------------------------------------------------------
SELECT prd_key, COUNT(*)
FROM
	(SELECT 
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt,
		pn.prd_end_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL) t -- Selecting only the current data that doesnt have an end date(historical data)
GROUP BY prd_key
HAVING COUNT(*) > 1

----------------------------------------------------------------------------------------
-- Remove unnessary columns and sort the col order
-- Rename cols to something friendly
-- Store the resulting dim table as a VIEW
----------------------------------------------------------------------------------------
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- If ERP switches from IDs like 210, 211 to P001, P002, your entire model breaks if you rely on natural keys.
	pn.prd_id AS product_id,                                                -- So even though we have prd_id here, its better to have a surrogate key
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	pn.prd_cost AS cost ,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL

----------------------------------------------------------------------------------------
-- Identify the object type
-- This is a FACT table, use DIM tables surrogate keys instead of IDs to easily connect FACT with DIMs
-- We need to join the 3 tables to get surroagte keys, this is called DATA LOOKUP
----------------------------------------------------------------------------------------
SELECT 
sd.sls_ord_num,
pr.product_key,
cu.customer_key,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

----------------------------------------------------------------------------------------
-- Rename the cols
-- sot cols into logical groups, normal order - (primary key, DIM keys), DATES , (Measures)
----------------------------------------------------------------------------------------
CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_nummber,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS sales_quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id