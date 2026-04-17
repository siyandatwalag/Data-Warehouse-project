/*
Quality checks

script purpose:
This script performs various qulaity checks for data consistency, accuracy, and standardization across the silver schamas. It includes checks for :
-Null or duplicate primary keys
-Unwanted spaces in string fields
-Data standardization and consistency.
-Invalid data ranges and order 
-Data consistency between related fields
*/

--check for nulls and duplicates
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--check unwanted spaces in strings 
SELECT prd_nm
from bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Data standardization & consistency 
SELECT DISTINCT cst_gndr from bronze.crm_cust_info
SELECT DISTINCT cst_marital_status from bronze.crm_cust_info

--check for nulls or negative numbers
select prd_cost, prd_id from bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--check for invalid date orders 
select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

--check for invalid date
select 
NULLIF(sls_ship_dt, 0)
from bronze.crm_sales_details
where LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

select sls_ship_dt, sls_due_dt, sls_order_dt
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or  sls_order_dt > sls_due_dt

--check data consistency between sales, quantity, and price 
-- sales = quantity *  price
-- valuers must notbe negative, zero or null
select distinct sls_sales AS old_sls_sales, sls_quantity, sls_price AS old_sls_price,

case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price)
	Then  abs(sls_price) * sls_quantity
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <= 0 
	THEN sls_sales / nullif(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
from bronze.crm_sales_details
where sls_sales	!= sls_quantity * sls_price
or sls_sales is null or sls_price is null or sls_quantity is null
or sls_sales <= 0 or sls_price <= 0 or sls_quantity <= 0
order by sls_sales, sls_quantity, sls_price

--identify out of range dates
SELECT DISTINCT bdate from bronze.erp_cust_az12
where bdate < '1924-01-01' OR bdate > GETDATE()

--data standaerization and consistency 
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	else 'n/a'
END AS gen
from bronze.erp_cust_az12

--data standardization and consistnecy 
select distinct cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
from bronze.erp_loc_a101
