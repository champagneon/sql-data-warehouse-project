/*
============================================================================
DDL Script: Create Gold Views
============================================================================
Script Purspose:
	This script creates views for the Gold layer in DWH.
	The gold layer represents the final dimension and fact tables.

	Each view performs transformations and combines data
	from the Silver layer and produces a clean, enriched and
	business-ready dataset.

Usage:
	- These views can be queried directly for analytics and reporting.
============================================================================
*/

drop view if exists gold.dim_customers;
create view gold.dim_customers as 
select
	row_number() over (order by cst_id) as customer_key,
	cci.cst_id as customer_id,
	cci.cst_key as customer_number,
	cci.cst_firstname as first_name,
	cci.cst_lastname as last_name,
	ela.cntry as country,
	cci.cst_marital_status as marital_status,
	case when cci.cst_gndr != 'N/A' then cci.cst_gndr -- CRM is the Master for gender info
		else coalesce(eca.gen, 'N/A')
	end as gender,
	eca.bdate as birthdate,
	cci.cst_create_date as create_date
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid;

drop view if exists gold.dim_products;
create view gold.dim_products as
select
	row_number() over (order by prd_start_dt, prd_key) as product_key,
	prd_id as product_id,
	prd_key as product_number,
	prd_nm as product_name,
	cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	prd_cost as cost,
	prd_line as product_line,
	prd_start_dt as start_date
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 pc
on cpi.cat_id = pc.id
where prd_end_dt is null;

drop view if exists gold.fact_sales;
create view gold.fact_sales as
select
	sls_ord_num as order_number,
	dp.product_key,
	dc.customer_key,
	sls_order_dt as order_date,
	sls_ship_dt as shipping_date,
	sls_due_dt as due_date,
	sls_sales as sales_amount,
	sls_quantity as quantity,
	sls_price as price
from silver.crm_sales_details csd
left join gold.dim_products dp
on csd.sls_prd_key = dp.product_number
left join gold.dim_customers dc
on csd.sls_cust_id = dc.customer_id;
