/*
====================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'LOAD DATA FILE INFILE' command to load data from CSV files to bronze tables.

Parameters:
    None.
  This stored procedure doesn not accept any parameters or return any values.

Usage Example:
   CALL load_bronze();
========================================================================
*/

DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT '==========================================';
        SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        SELECT CONCAT('Error Message: ', MESSAGE_TEXT)
        FROM information_schema.ERRORS
        LIMIT 1;
        SELECT '==========================================';
    END;

    SET batch_start_time = NOW();

    SELECT '================================================';
    SELECT 'Loading Bronze Layer';
    SELECT '================================================';

    -- CRM Tables
    SELECT '------------------------------------------------';
    SELECT 'Loading CRM Tables';
    SELECT '------------------------------------------------';

    -- CRM_CUST_INFO
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    SELECT '>> Inserting Data Into: bronze.crm_cust_info';
    LOAD DATA LOCAL INFILE '/your/path/to/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');
    SELECT '>> -------------';

    -- CRM_PRD_INFO
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    SELECT '>> Inserting Data Into: bronze.crm_prd_info';
    LOAD DATA LOCAL INFILE '/your/path/to/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');
    SELECT '>> -------------';

    -- CRM_SALES_DETAILS
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    SELECT '>> Inserting Data Into: bronze.crm_sales_details';
    LOAD DATA LOCAL INFILE '/your/path/to/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');
    SELECT '>> -------------';

    -- ERP LOC_A101
    SELECT '------------------------------------------------';
    SELECT 'Loading ERP Tables';
    SELECT '------------------------------------------------';

    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    SELECT '>> Inserting Data Into: bronze.erp_loc_a101';
    LOAD DATA LOCAL INFILE '/your/path/to/loc_a101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');
    SELECT '>> -------------';

    -- ERP CUST_AZ12
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    SELECT '>> Inserting Data Into: bronze.erp_cust_az12';
    LOAD DATA LOCAL INFILE '/your/path/to/cust_az12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');
    SELECT '>> -------------';

    -- ERP PX_CAT_G1V2
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    SELECT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    LOAD DATA LOCAL INFILE '/your/path/to/px_cat_g1v2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');
    SELECT '>> -------------';

    SET batch_end_time = NOW();
    SELECT '==========================================';
    SELECT 'Loading Bronze Layer is Completed';
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds');
    SELECT '==========================================';

END$$

DELIMITER ;


