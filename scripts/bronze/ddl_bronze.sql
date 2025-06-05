/*
============================================================================
DDL Script: Create bronze tables
============================================================================
Script Purspose:
  Creates tables in bronze schema, dropping any tables that already exists.
  Run this script to re-define the DDL Sctructure of bronze tables
============================================================================
*/


drop table if exists bronze.crm_cust_info;
create table bronze.crm_cust_info (
	cst_id INT,
	cst_key varchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_marital_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date date
);

drop table if exists bronze.crm_prd_info;
create table bronze.crm_prd_info (
	prd_id int,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost int,
	prd_line varchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime
);

drop table if exists bronze.crm_sales_details;
create table bronze.crm_sales_details (
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);


drop table if exists bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
	CID varchar(50),
	BDATE date,
	GEN varchar(50)
);

drop table if exists bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
	CID varchar(50),
	CNTRY varchar(50)
);

drop table if exists bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2 (
	ID varchar(50),
	CAT varchar(50),
	SUBCAT varchar(50),
	MAINTENANCE varchar(50)
);



TRUNCATE table bronze.crm_cust_info;
LOAD DATA LOCAL INFILE 'C:\\Users\\dyuma\\Desktop\\sql-data-warehouse-project\\datasets\\source_crm\\cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

TRUNCATE table bronze.crm_prd_info;
LOAD DATA LOCAL infile 'C:\\Users\\dyuma\\Desktop\\sql-data-warehouse-project\\datasets\\source_crm\\prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS terminated by ','
IGNORE 1 rows;

TRUNCATE table bronze.crm_sales_details;
LOAD DATA LOCAL INFILE 'C:\\Users\\dyuma\\Desktop\\sql-data-warehouse-project\\datasets\\source_crm\\sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

TRUNCATE table bronze.erp_cust_az12;
LOAD DATA LOCAL INFILE 'C:\\Users\\dyuma\\Desktop\\sql-data-warehouse-project\\datasets\\source_erp\\cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

TRUNCATE table bronze.erp_loc_a101;
LOAD DATA LOCAL INFILE 'C:\\Users\\dyuma\\Desktop\\sql-data-warehouse-project\\datasets\\source_erp\\loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

TRUNCATE table bronze.erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE 'C:\\Users\\dyuma\\Desktop\\sql-data-warehouse-project\\datasets\\source_erp\\px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;
