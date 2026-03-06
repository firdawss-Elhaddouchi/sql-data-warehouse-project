/* ============================================================
   DATA EXPLORATION & DATA QUALITY CHECKS
   Layer: BRONZE
   Goal: Identify data issues before transforming to SILVER
   ============================================================ 

Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
===============================================================
*/


/* ============================================================
   TABLE: CRM_CUST_INFO
   ============================================================ */

/*-------------------------------------------------------------
1. Check for NULLs or Duplicates in Primary Key (cst_id)
Expectation: No results
-------------------------------------------------------------*/
SELECT 
    cst_id,
    COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

/*
Detected Issues:
cst_id
------
29449 | 2
29473 | 2
29433 | 2
NULL  | 3
29483 | 2
29466 | 3
*/


/*-------------------------------------------------------------
2. Check for Unwanted Spaces
If the original value differs from the trimmed value,
then spaces exist at the beginning or end.
Expectation: No results
-------------------------------------------------------------*/

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);
/* No spaces detected */


/*-------------------------------------------------------------
3. Data Standardization & Consistency
Use clear values instead of abbreviations.
-------------------------------------------------------------*/

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

/*
NULL
F
M
*/

SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info;

/*
S
NULL
M
*/


/* ============================================================
   TABLE: CRM_PRD_INFO
   ============================================================ */

SELECT *
FROM bronze.crm_prd_info;


/*-------------------------------------------------------------
1. Check for Duplicates or NULLs in Primary Key
Expectation: No results
-------------------------------------------------------------*/

SELECT 
    prd_id,
    COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


/*-------------------------------------------------------------
2. Check for Unwanted Spaces
-------------------------------------------------------------*/

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


/*-------------------------------------------------------------
3. Check for NULLs or Negative Values
-------------------------------------------------------------*/

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


/*-------------------------------------------------------------
4. Data Standardization
-------------------------------------------------------------*/

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

/*
M
R
S
T
*/


/*-------------------------------------------------------------
5. Check for Invalid Date Ranges
-------------------------------------------------------------*/

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


/*
Potential Solutions:

Solution 1
Switch start and end dates
⚠ May create overlapping records.

Solution 2
Derive end date from next record:
End Date = Next Start Date - 1
*/


SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt)
        OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
        AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');



/* ============================================================
   TABLE: CRM_SALES_DETAILS
   ============================================================ */

/*-------------------------------------------------------------
1. Check for Unwanted Spaces
-------------------------------------------------------------*/

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);


/*-------------------------------------------------------------
2. Validate Foreign Keys
Ensure product and customer exist in reference tables
-------------------------------------------------------------*/

SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key FROM silver.crm_prd_info
);

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
);

/* Result: No issues detected */


/*-------------------------------------------------------------
3. Check for Invalid Dates
-------------------------------------------------------------*/

SELECT
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;

/*
Fix:
Replace invalid values (0 or wrong length)
with NULL
*/


/*-------------------------------------------------------------
4. Check Date Logic
-------------------------------------------------------------*/

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


/*-------------------------------------------------------------
5. Check Sales Consistency
Rule:
Sales = Quantity × Price
-------------------------------------------------------------*/

SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


/*
Possible Fixes

Option 1
Fix data in the source system.

Option 2
Fix in the Data Warehouse.

Rules:
- If Sales is NULL, zero, or negative → derive from Quantity × Price
- If Price is NULL or zero → derive from Sales / Quantity
- If Price is negative → convert to positive
*/


SELECT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,

    CASE
        WHEN sls_sales IS NULL
          OR sls_sales <= 0
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;



/* ============================================================
   TABLE: ERP_CUST_AZ12
   ============================================================ */

SELECT
    cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;


/* Remove prefix "NAS" from Customer ID */

SELECT
    CASE
        WHEN cid LIKE 'NAS%'
        THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;


/* Check for unrealistic birth dates */

SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();


/* Gender standardization */

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

/*
NULL
F
Male
Female
M
*/



/* ============================================================
   TABLE: ERP_LOC_A101
   ============================================================ */

SELECT
    cid,
    cntry
FROM bronze.erp_loc_a101;


/* Remove dash from customer key */

SELECT
    REPLACE(cid,'-','') AS cid,
    cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN
(
    SELECT cst_key FROM silver.crm_cust_info
);


/* Country Standardization */

SELECT DISTINCT
    cntry AS old_cntry,
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;



/* ============================================================
   TABLE: ERP_PX_CAT_G1V2
   ============================================================ */

SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;


/* Check for unwanted spaces */

SELECT *
FROM bronze.erp_px_cat_g1V2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);


/* Data Standardization */

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1V2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1V2;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1V2;
