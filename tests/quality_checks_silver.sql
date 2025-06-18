/*
==========================================================
Quality Checks 
==========================================================
Script Purpose:
	Ensure the purity of the data before and after
	inserting from Bronze into Silver.

	Each check can be adjusted according to tables
	(e.g. can check prd_id instead of cst_id etc.)
*/

-- check for nulls or duplicates in primary key
-- expectation: no result 
select
cst_id,
count(*)
from silver.crm_cust_info cci
group by cci.cst_id
having count(*) > 1;

-- check for unwanted spaces
-- can check last_name and other columns too
-- expectation: no result 
select cst_firstname
from silver.crm_cust_info cci 
where cst_firstname != trim(cst_firstname);

-- data standartization & consistency
-- cst_gndr, cst_marital_status etc.
select distinct cst_gndr
from silver.crm_cust_info cci;

-- check for nulls or negative numbers in cost
-- expectation: no result 
select prd_cost
from silver.crm_prd_info cpi 
where prd_cost < 0 or prd_cost is null;

-- check for invalid date orders 1
select *
from silver.crm_prd_info cpi 
where prd_end_dt < prd_start_dt
order by prd_key;


-- check for invalid date orders 2
select *
from silver.crm_sales_details csd 
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

select 
nullif(sls_due_dt, 0) as sls_due_dt
from bronze.crm_sales_details csd
where 
sls_due_dt <= 0 
or length(sls_due_dt) != 8 
or sls_due_dt > 20300101 
or sls_due_dt < 19900101;

-- check data consistency (sales, quantity, price)
-- >> sales = quantity * price
-- >> values must not be null, zero or negative
select 
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-----------------
select *
from (
	select *,
	row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
	from silver.crm_cust_info
	where cst_id is not null
)t where flag_last = 1;
-----------------

