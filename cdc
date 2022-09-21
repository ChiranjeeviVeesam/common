------------------------------------------------------------------------------------
select * 
from test_ds.hoc_gsheet;


------------------------------------------------------------------------------------
create or replace table bdcsproject.test_ds.hoc_cdc
as 
select * , 
current_timestamp as created_dt,
current_timestamp as last_updated_dt, 
'Initial Load' as last_updated_by, 
from bdcsproject.test_ds.hoc_gsheet;

------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
create or replace procedure `bdcsproject.test_ds.sp_cdc`() 
BEGIN 

create temp table cdc_table
as 
select *,
       (CASE 
         WHEN mod_rec.country is null THEN 'Insert' 
         WHEN new_mod_rec.country_new is null THEN 'Delete' 
         WHEN mod_rec.country = new_mod_rec.country_new THEN 'Update' 
         else 'Undefined'
       END) as Operation_flag
from 

  (
  -----------------Records modified in Source ----------------------
  select country,hos,hos_nm,hog,hog_nm
  from
      (
      select country,hos,hos_nm,hog,hog_nm
      from test_ds.hoc_cdc
      except distinct
      select country,hos,hos_nm,hog,hog_nm
      from test_ds.hoc_gsheet
      ) 
 ) as mod_rec
 
 FULL JOIN 
 
 
 (
  -----------------New/Modified Records in Source ----------------------
  select  country as country_new,hos as hos_new,hos_nm as hos_nm_new,hog as hog_new,hog_nm as hog_nm_new
  from 
    (
    select country,hos,hos_nm,hog,hog_nm
    from test_ds.hoc_gsheet
    except distinct
    select country,hos,hos_nm,hog,hog_nm
    from test_ds.hoc_cdc
    ) 
  ) as new_mod_rec
  
  
  ON new_mod_rec.country_new = mod_rec.country;
 
#Insert records  
 insert into test_ds.hoc_cdc 
 select country_new as country, hos_new as hos, hos_nm_new as hos_nm, 
 hog_new as hog, hog_nm_new as hog_nm, 
current_timestamp as created_dt,
current_timestamp as last_updated_dt, 
'Stored Proc' as last_updated_by
 from cdc_table where operation_flag='Insert';
  
#Delete records
delete from test_ds.hoc_cdc 
where country 
in (select country from cdc_table where operation_flag='Delete');

#Update records
Update test_ds.hoc_cdc hoc_cdc 
set hos = hos_new,
    hos_nm = hos_nm_new, 
    hog = hog_new, 
    hog_nm = hog_nm_new,
    last_updated_dt=current_timestamp,
    last_updated_by = 'Stored Proc'
    
from cdc_table tmp
where tmp.operation_flag = 'Update' and 
hoc_cdc.country = tmp.country;

#Update run_control_table
insert into test_ds.run_control_tbl
select 'hoc_cdc' as tbl_nm, 
       'hoc_gsheet' as src_tbl_nm,
       sum(if(operation_flag='Insert',1,0)) as rec_inserted,
       sum(if(operation_flag='Delete',1,0)) as rec_deleted,
       sum(if(operation_flag='Update',1,0)) as rec_updated,
       current_timestamp as run_dt
       from cdc_table;
        


drop  table cdc_table;
END;

------------------------------------------------------------------------------------
call `bdcsproject.test_ds.sp_cdc`()
