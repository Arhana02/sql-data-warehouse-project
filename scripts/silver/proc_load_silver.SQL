/*
===================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===================================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
          - Truncates Silver tables.
          - Inserts transformed and cleansed data from Bronze into Silver Tables.

Parameters:
    None.
      This stored procedure does not accept any parameters or return any values.

Usage Example:
    Call silver.silver_load_silver;
===================================================================================================
*/

DELIMITER //

DROP PROCEDURE IF EXISTS silver.silver_load_silver;

CREATE PROCEDURE silver.silver_load_silver()
BEGIN
    -- Declare variables
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    DECLARE exit handler for SQLEXCEPTION
    BEGIN
        SELECT '==========================================' AS msg;
        SELECT 'ERROR OCCURRED DURING LOADING SILVER LAYER' AS msg;
        SELECT '==========================================' AS msg;
    END;

    -- Start batch
    SET batch_start_time = NOW();

    SELECT '================================================' AS msg;
    SELECT 'Loading Silver Layer' AS msg;
    SELECT '================================================' AS msg;

    SELECT '------------------------------------------------' AS msg;
    SELECT 'Loading CRM Tables' AS msg;
    SELECT '------------------------------------------------' AS msg;
    
	  -- 1️⃣ Load silver.crm_cust_info
    SET start_time = NOW();
    SELECT '>> Truncating table: silver.crm_cust_info' AS msg;
	  TRUNCATE TABLE silver.crm_cust_info;
	  SELECT '>> Inserting data into: silver.crm_cust_info' AS msg;
	  INSERT INTO silver.crm_cust_info(
  	cst_id,
  	cst_key,
  	cst_firstname,
  	cst_lastname,
  	cst_marital_status,
  	cst_gndr,
  	cst_create_date,
  	dwh_create_date)
  	SELECT
  	cst_id,
  	cst_key,
  	TRIM(cst_firstname) AS cst_firstname,
  	TRIM(cst_lastname) AS cst_lastname,
	  CASE
	    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	  ELSE 'n/a'
	  END AS  cst_marital_status, -- Normalize marital status values to readable format
	  CASE
    	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	  ELSE 'n/a'
  	END AS cst_gndr, -- Normalise gender values to readable format
  	cst_create_date,
   	CURRENT_TIMESTAMP AS dwh_create_date
	  FROM(
  	SELECT
  	*,
  	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
  	FROM bronze.crm_cust_info
  	)t WHERE flag_last = 1; -- Select the most recent record per customer
    
	  SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;
    
    -- 2️⃣ Load silver.crm_prd_info
    SET start_time = NOW();
  	SELECT '>> Truncating table: silver.crm_prd_info' AS msg;
  	TRUNCATE TABLE silver.crm_prd_info;
  	SELECT '>> Inserting data into: silver.crm_prd_info' AS msg;
  	INSERT INTO silver.crm_prd_info(
  	prd_id,
  	cat_id,
  	prd_key,
  	prd_nm,
  	prd_cost,
  	prd_line,
  	prd_start_dt,
  	prd_end_dt,
  	dwh_create_date
  	)
  	SELECT
  	prd_id, 
  	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
  	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- Extract product key
  	prd_nm,
  	IFNULL(prd_cost, 0) AS prd_cost,
	  CASE UPPER(TRIM(prd_line))
		   WHEN 'M' THEN 'Mountain'
		   WHEN 'R' THEN 'Road'
		   WHEN 'S' THEN 'Other Sales'
		   WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
  	END AS prd_line, -- Map product line codes to descriptive values
  	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	  CAST(
	  LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-INTERVAL 1 DAY
	  AS date
	  ) AS prd_end_dt, -- Calculate end date as one day before the next start date
	  CURRENT_TIMESTAMP AS dwh_create_date
	  FROM bronze.crm_prd_info;
 
	  SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;
	
	  -- 3️⃣ Load silver.crm_sales_details
    SET start_time = NOW();
    SELECT '>> Truncating table: silver.crm_sales_details' AS msg;
	  TRUNCATE TABLE silver.crm_sales_details;
	  SELECT '>> Inserting data into: silver.crm_sales_details' AS msg;
	  INSERT INTO silver.crm_sales_details(
  	sls_ord_num,
  	sls_prd_key,
  	sales_cust_id,
  	sls_order_dt,
  	sls_ship_dt,
  	sls_due_dt,
  	sls_sales,
  	sls_quantity,
  	sls_price
  	)
  	SELECT
  	sls_ord_num,
  	sls_prd_key,
  	sales_cust_id,
  	CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN null
		ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
	  END AS sls_order_dt,
	  CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN null
    ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
	  END AS sls_ship_dt,
	  CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN null
		ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
	  END AS sls_due_dt,
	  CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	  END sls_sales, -- Recalculate sales if original value is missing or incorrect
	  sls_quantity,
	  CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/ NULLIF(sls_quantity, 0)
		ELSE sls_price -- Derive price if original value is invalid
	  END AS sls_price
	  FROM bronze.crm_sales_details;
	
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;
	
	  -- 4️⃣ Load silver.erp_cust_az12
    SET start_time = NOW();
	  SELECT '>> Truncating table: silver.erp_cust_az12' AS msg;
  	TRUNCATE TABLE silver.erp_cust_az12;
  	SELECT '>> Inserting data into: silver.erp_cust_az12' AS msg;
  	INSERT INTO silver.erp_cust_az12(
  	cid,
  	bdate,
  	gen
  	)
  	SELECT
  	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
  	ELSE cid
  	END cid,
  	CASE WHEN bdate > CURDATE() THEN null
  	ELSE bdate
  	END AS bdate, -- Set future birthdates to NULL
  	CASE 
      WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
  	  WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
  	ELSE 'n/a'
  	END AS gen -- Normalise gender values and handle unknown cases
	  FROM bronze.erp_cust_az12;
	
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

	  -- 5️⃣ Load silver.erp_loc_a101
    SET start_time = NOW();
	  SELECT '>> Truncating table: silver.loc_a101' AS msg;
	  TRUNCATE TABLE silver.loc_a101;
	  SELECT '>> Inserting data into: silver.loc_a101' AS msg;
	  INSERT INTO silver.loc_a101(
	  cid,
	  cntry
  	)
  	SELECT
  	REPLACE(cid, '-', '') AS cid,
  	CASE   
      WHEN TRIM(cntry) = 'DE' THEN 'Germany'
      WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
      WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
	  END AS cntry -- Nomalise and Handle missing or blank country codes
	  FROM bronze.loc_a101;
	  SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

    -- 6️⃣ Load silver.erp_px_cat_g1v2
    SET start_time = NOW();
	  SELECT '>> Truncating table: silver.px_cat_g1v2' AS msg;
	  TRUNCATE TABLE silver.px_cat_g1v2;
	  SELECT '>> Inserting data into: silver.px_cat_g1v2' AS msg;
	  INSERT INTO silver.px_cat_g1v2(
	  id,
  	cat,
  	subcat,
  	maintenance
  	)
  	SELECT
  	id,
  	cat,
  	subcat,
  	maintenance
  	FROM bronze.px_cat_g1v2;
    
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS msg;

	  -- End batch
    SET batch_end_time = NOW();

    SELECT '==========================================' AS msg;
    SELECT 'Loading Silver Layer is Completed' AS msg;
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS msg;
    SELECT '==========================================' AS msg;
END //




