truncate table silver.crm_cust_info;
	-- inserting clean data from bronze to silver =====================================================================================
	insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			 when upper(trim(cst_marital_status)) = 'M' then 'Married'
			 else 'N/A'
		end as cst_marital_status,
		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr)) = 'M' then 'Male'
			 else 'N/A'
		end as cst_gndr,
		cst_create_date
	from (select *, 
		row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null
	)t where flag_last = 1;
	-- inserted =========================================================================================================================

	truncate table silver.crm_prd_info;
	-- inserting clean data to silver =======================================================================================
	insert into silver.crm_prd_info (
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
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id, -- extract categor ID
		substring(prd_key, 7, length(prd_key)) as prd_key, -- extract product KEY
		prd_nm,
		prd_cost,
		case upper(trim(prd_line))
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'N/A'
		end as prd_line, -- map product lines codes to descriptive values
		date(prd_start_dt) as prd_start_dt,
		lead(date(prd_start_dt)) over (partition by prd_key order by prd_start_dt) -interval 1 day as prd_end_dt -- set end date to 1 day before next start date
	from bronze.crm_prd_info cpi;
	-- inserted ==============================================================================================================

	truncate table silver.crm_sales_details;
	-- inserting clean data into silver =====================================================================================
	insert into silver.crm_sales_details (
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
	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			 when sls_order_dt <= 0 or length(sls_order_dt) != 8 then null
			 else str_to_date(cast(sls_order_dt as char), '%Y%m%d')
		end as sls_order_dt,
		case 
			 when sls_ship_dt <= 0 or length(sls_ship_dt) != 8 then null
			 else str_to_date(cast(sls_ship_dt as char), '%Y%m%d')
		end as sls_ship_dt,
		case 
			 when sls_due_dt <= 0 or length(sls_due_dt) != 8 then null
			 else str_to_date(cast(sls_due_dt as char), '%Y%m%d')
		end as sls_due_dt, -- remove incorrect dates, convert int to date
		case 
			 when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) and sls_price is not null and sls_price != 0
				then sls_quantity * abs(sls_price)
			 else sls_sales
		end as sls_sales, -- recalculating incorrect sales 
		sls_quantity,
		case 
			 when sls_price is null or sls_price <= 0
			 then sls_sales / nullif(sls_quantity, 0)
			 else sls_price
		end as sls_price -- recalculating incorrect prices
	from bronze.crm_sales_details csd;
	-- inserted ================================================================================================================
	
	truncate table silver.erp_cust_az12;
	-- inserting clean data into silver =============================================================
	insert into silver.erp_cust_az12 (cid, bdate, gen)
	select
	case
		when cid like 'NAS%' then substring(cid, 4, length(cid))
		else cid
	end as cid, -- remove first 3 letters, not specified what they are for
	case 
		when bdate > now() then null
		else bdate
	end as bdate, -- remove future birthdates. although leaving people >100 y.o (consult experts)
	case 
		WHEN UPPER(gen) LIKE 'M%' AND UPPER(gen) NOT LIKE 'FEMALE%' THEN 'Male'
		WHEN UPPER(gen) LIKE 'F%' AND UPPER(gen) NOT LIKE 'MALE%' THEN 'Female'
		ELSE 'N/A'
	END AS gen -- dirty data, NBSPs
			   -- couldn't use upper(trim()), using 'like' for now
	from bronze.erp_cust_az12 eca;
	-- inserted =====================================================================================

	truncate table silver.erp_loc_a101;
	-- inserting clean data into silver ===================================================================================================
	INSERT INTO silver.erp_loc_a101 (cid, cntry)
	WITH CleanedSourceData AS (
	    SELECT
	        REPLACE(ela.cid, '-', '') AS cleaned_cid, -- Очистка CID здесь
	        -- Все функции очистки для CNTY применяем ОДИН раз
	        TRIM(REPLACE(REPLACE(REPLACE(REPLACE(ela.cntry, UNHEX('C2A0'), ''), '\r', ''), '\n', ''), '\t', '')) AS cleaned_cntry,
	        ela.cntry AS original_cntry -- Сохраняем оригинальное значение CNTY для проверки IS NULL
	    FROM
	        bronze.erp_loc_a101 ela
	)
	SELECT
	    csd.cleaned_cid,
	    CASE
	        -- Используем уже очищенное значение cleaned_cntry для всех сравнений
	        WHEN csd.cleaned_cntry LIKE 'US%' OR csd.cleaned_cntry LIKE 'United States%' THEN 'United States'
	        WHEN csd.cleaned_cntry LIKE 'DE%' OR csd.cleaned_cntry LIKE 'Germany%' THEN 'Germany'
	        -- Проверяем на пустоту очищенного поля ИЛИ на NULL в оригинальном поле
	        WHEN csd.cleaned_cntry = '' OR csd.original_cntry IS NULL THEN 'N/A'
	        ELSE csd.cleaned_cntry
	    END AS cntry
	FROM
	    CleanedSourceData csd;
	-- inserted ===========================================================================================================================
	
	truncate table silver.erp_px_cat_g1v2;
	-- inserting cleand data into silver =================================================================================
	insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	select
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2 epcgv;
	-- inserted ==========================================================================================================
