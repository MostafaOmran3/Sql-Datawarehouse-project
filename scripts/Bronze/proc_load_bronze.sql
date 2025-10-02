
create or alter procedure bronze.load_bronze as 
	begin
	begin try
		  declare @start_time datetime , @end_time datetime ,@batch_start_time datetime, @batch_end_time datetime
					print '=============================================='
					print 'truncating table bronze.crm_cust_info '
					print '======================================'
					set @batch_start_time =getdate()
					set @start_time = getdate()
			truncate table bronze.crm_cust_info 
					print '==============================='
					print 'inserting data into bronze.crm_cust_info'
			bulk insert bronze.crm_cust_info  
			from 'C:\Users\omran\OneDrive\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with (
			firstrow = 2,
			fieldterminator =',',
			tablock) 
					set @end_time = GETDATE()
				   print'loading time:' + cast (datediff(second,@start_time,@end_time) as nvarchar) +'seconds'
				   print 'inserting data into bronze.crm_cust_info done !!'
		   
		   

				   print '=============================================='
				   print 'truncating table bronze.crm_prd_info '
				   print '======================================'

				   set @start_time =GETDATE()
			truncate table bronze.crm_prd_info 

				   print '=============================================='
				   print 'inserting data into bronze.crm_prd_info '
				   print '======================================'
			bulk insert bronze.crm_prd_info
			from 'C:\Users\omran\OneDrive\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with (
			firstrow = 2,
			fieldterminator =',',
			tablock) 
				  set @end_time = GETDATE()
				  print'loading time:' + cast (datediff(second,@start_time,@end_time) as nvarchar) +'seconds'

				  print '=============================================='
				  print 'inserting data into bronze.crm_cust_info done !! '
				  print '======================================'
				  print '=============================================='
				  print 'truncating table bronze.crm_prd_info '
				  print '======================================'
				  set @start_time= GETDATE()
			 truncate table bronze.crm_sales_details

				  print'======================================'

				  print'inserting data into bronze.crm_sales_details'
			 bulk insert bronze.crm_sales_details 
			 from 'C:\Users\omran\OneDrive\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			 with (
			 firstrow = 2,
			 fieldterminator =',',
			 tablock) 
				  set @end_time= GETDATE()
				  print'loading time:' + cast (datediff(second,@start_time,@end_time) as nvarchar) +'seconds'

				  print'======================================'
				  print 'inserting data into bronze.crm_sales_details done!!'
				  print'======================================'

				  print 'truncating table bronze.erp_cust_az12'
				  set @start_time = GETDATE()
			 truncate table bronze.erp_cust_az12
				  print 'inserting data into bronze.erp_cust_az12'
			 bulk insert bronze.erp_cust_az12 
			 from 'C:\Users\omran\OneDrive\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			 with (
			 firstrow = 2,
			 fieldterminator =',',
			 tablock) 
				 set @end_time = getdate()
				 print'loading time:' + cast (datediff(second,@start_time,@end_time) as nvarchar) +'seconds'

				 print 'inserting data into bronze.erp_cust_az12 done !!'
	   			 print 'truncating table bronze.erp_loc_a101 '
				 set @start_time= GETDATE()
			 truncate table bronze.erp_loc_a101 

				 print 'inserting data into bronze.erp_loc_a101'
			 bulk insert bronze.erp_loc_a101
			 from 'C:\Users\omran\OneDrive\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			 with (
			 firstrow = 2,
			 fieldterminator =',',
			 tablock)
				 set @end_time= GETDATE()
				 print'loading time:' + cast (datediff(second,@start_time,@end_time) as nvarchar) +'seconds'

				 print 'inserting data into bronze.erp_loc_a101 done !!'
				 print 'truncating table bronze.erp_px_cat_g1v2 '
				 set @start_time= getdate()

			 truncate table bronze.erp_px_cat_g1v2

				 print 'inserting data into bronze.erp_px_cat_g1v2'
			 bulk insert bronze.erp_px_cat_g1v2
			 from 'C:\Users\omran\OneDrive\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			 with (
			 firstrow = 2,
			 fieldterminator =',',
			 tablock) 
				set @end_time= getdate()
				set @batch_start_time =getdate()
				print 'loading time:' + cast (datediff(second,@start_time,@end_time) as nvarchar) +'seconds'
				print 'inserting data into bronze.erp_px_cat_g1v2 done !!'
				print 'all batch loading time:' + cast (datediff(second,@batch_start_time,@batch_end_time) as nvarchar) +'seconds'

  end try 
	 begin catch
				print 'error occured during loading bronze layer '
				print 'error message' + error_message()
				print 'error message' + cast (error_number() as nvarchar)
				print 'error message' + cast (error_state() as nvarchar)
	 end catch

end 
