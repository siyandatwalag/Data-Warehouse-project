/*
Stored procedure: Load silver layer (broze -> silver)
purpose:
this  stored procedure performs the ETL(Extract, Transform, Load) process to populate the silver schema tabnles from the bronze schema.

action perfomed:
Truncates silver tables
Inserts transformed and cleansed data from bronze into nsilver tables

Parameters:
None...This  stored procedure does not accpet any parameters or return any values.

usage example:
EXEC silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '============================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================';
		PRINT 'Loading CRM Tables';
		PRINT '===========================';
	--clean and load silver.crm_cust_info
	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info
	PRINT '>>Inserting Data Into: silver.crm_cust_info ';
	INSERT INTO silver.crm_cust_info(
	cst_id, 
	cst_key,
	cst_firstanme,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT cst_id,
	cst_key, 
	TRIM(cst_firstanme) AS cst_firstanme,
	TRIM(cst_lastname) AS cst_lastname,

	CASE WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'SINGLE'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
		ELSE 'N/A'
	END as cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'FEMALE'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
		ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date
	FROM(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	)t where flag_last = 1
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';


	--clean and load silver.crm_prd_info
	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info
	PRINT '>>Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)

	select 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --extract category id
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- extract product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
		WHEN TRIM(UPPER(prd_line))  = 'R' THEN 'Road'
		WHEN TRIM(UPPER(prd_line))  = 'S' THEN 'Other Sales'
		WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line, --map product line code to descriptive values
	CAST(prd_start_dt AS DATE) as prd_start_dt, --calculate end date as one day before the next start date
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) as prd_end_dt
	from bronze.crm_prd_info
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';

	--clean and load silver.crm_sales_details
	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>>Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)

	SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price)
		Then  abs(sls_price) * sls_quantity
		else sls_sales
	end as sls_sales, -- Recalculate slaes if original value iis missing or incorrect
	sls_quantity,
	case when sls_price is null or sls_price <= 0 
		THEN sls_sales / nullif(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price -- Derive price if original value is invalid
	FROM bronze.crm_sales_details
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';


	--clean and load silver.erp_cust_az12
	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT '>>Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen)
	select
	case when cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Remove 'NAS' prefix if present  
		ELSE cid
	END cid ,

	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate, -- Set future birthdates to null
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		else 'n/a'
	END AS gen -- Normalize gender values and handle unknown cases
	from bronze.erp_cust_az12
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';


	--clean and load silver.erp_loc_a101
	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT '>>Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry)

	select replace (cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END cntry -- Normalize and handle missingg or blank country codes
	from bronze.erp_loc_a101
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';


	--clean and load silver.erp_px_cat_g1v2
	SET @start_time =  GETDATE()
	PRINT '>> TRUNCATING TABLE silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance)

	select id,
	cat,
	-- split maintenance into two columns 
	LEFT(maintenance, CHARINDEX(',', maintenance) -1) AS subcat,
	RIGHT(maintenance, LEN(maintenance) - CHARINDEX(',', maintenance)) AS maintenance
	from bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
	PRINT'--------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT'Batch load duration ' + CAST(DATEDIFF( second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'Seconds';
		END TRY
		BEGIN CATCH
			PRINT '========================'
			PRINT 'Error Occured during loading bronze layer'
			PRINT 'Error Message' + Error_Message();
			PRINT 'Error Message' + CAST(Error_NUMBER() AS NVARCHAR);
			PRINT '========================'
		END CATCH
END
