CREATE OR REPLACE PROCEDURE AIGI_EDR_COMMONMODEL.UTILITY.USP_LOAD_SCDTYPE1_TBL("PROC_CONTEXT_DB" VARCHAR(255), "PROC_CONTEXT_SCHEMA" VARCHAR(255), "TABLE_NAME" VARCHAR(255), "HKEY" VARCHAR(255), "SRC_SYS_CD" VARCHAR(50), "BUS_UNIT_CD" VARCHAR(50))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    // -- ================================================================================================================================================
    // -- Revision History:
    // -- Date          Author              Story		Description
    // -- 10th Apr''21	Swati Gupta						Procedure to load HUB and LINK tables for vault
    // -- 19th Apr''21	Lalchand Khilwani				Audit Logging, Error Recovery with restartability implementation and Procedure Execution Status Check for PID
    // -- 28th Jul''21	Lalchand Khilwani	ED14664		Changes for the Source System Cd level implmentation
    // -- 28th Mar''24	Akshay Nemade				Error Message Standerdization
    // -- ================================================================================================================================================
    // -- Parameters:	
	// -- PROC_CONTEXT_DB		- The Procedure context Database for referencing the tables
	// -- PROC_CONTEXT_SCHEMA	- The Procedure context schema for referencing the tables
	// -- TABLE_NAME    		- Name of HUB/LINK table
	// -- HKey					- Hash Key of the table
	// -- SRC_SYS_CD			- Source System Cd that needs to be processed in this invocation i.e. PW, AL, DXC-AsrClm, etc
	// -- BUS_UNIT_CD			- The Business Unit Code to process data for and make a batch control entry
	// --
	// -- Returns:  Success or Failure
	// -- Important notes(if any): 
	// -- Execution of this procedure may take an extended period of time for very 
	// -- large datasets, or for datasets with a wide variety of document attributes
	// -- (since the view will have a large number of columns)
	// -- ================================================================================================================================================

	function Proc_Status_Update()
	{
		// to record what is the status on the procedure , is it failed or sucess
        var v_proc_end_time = (new Date().getUTCFullYear()) +"-"+ parseInt((new Date().getUTCMonth())+1).toString().padStart(2,0) +"-"+ (new Date().getUTCDate()).toString().padStart(2,0) +" "+ (new Date().getUTCHours()).toString().padStart(2,0) +":"+ (new Date().getUTCMinutes()).toString().padStart(2,0) +":"+(new Date().getUTCSeconds()).toString().padStart(2,0);
		
		if(v_proc_status_marked)
		{
			return ''Already Marked'';
		}
		
		if(v_err_flg)
		{
			var update_proc_status= `UPDATE `+v_proc_status_table+` SET PROC_EXEC_START_TS_UTC=''`+v_proc_start_time+`'',PROC_EXEC_END_TS_UTC=''`+v_proc_start_time+`'',EXEC_STATUS=''`+v_status+`'',UPDATE_TS_UTC=convert_timezone(''UTC'',current_timestamp())::timestamptz WHERE PID=''`+v_pid+`'' AND PROC_NAME=''`+v_proc_name+`'' AND TARGET_TABLE=''`+v_table_name+`'' AND EXEC_STATUS=''FAILED'';;`;
			
			snowflake.execute({sqlText:update_proc_status});
		}
		else
		{
			var insert_proc_status  = `INSERT INTO `+v_proc_status_table+` (PID, PROC_NAME, TARGET_TABLE, PROC_EXEC_START_TS_UTC, PROC_EXEC_END_TS_UTC, EXEC_STATUS, UPDATE_TS_UTC, LOAD_TS_UTC) VALUES
										(''`+v_pid+`'',''`+v_proc_name+`'',''`+v_table_name+`'',''`+v_proc_start_time+`'',''`+v_proc_end_time+`'',''`+v_status+`'',convert_timezone(''UTC'',current_timestamp())::timestamptz,convert_timezone(''UTC'',current_timestamp())::timestamptz);`;
			snowflake.execute({sqlText:insert_proc_status});
		}
		v_proc_status_marked=Boolean(true);		
		return ''SUCCESS: Proc_Status_Updated''
	}

    // main procedure starts here
try 
{ 
    // Varaible assignment 
    var v_proc_start_time = (new Date().getUTCFullYear()) +"-"+ parseInt((new Date().getUTCMonth())+1).toString().padStart(2,0) +"-"+ (new Date().getUTCDate()).toString().padStart(2,0) +" "+ (new Date().getUTCHours()).toString().padStart(2,0) +":"+ (new Date().getUTCMinutes()).toString().padStart(2,0) +":"+(new Date().getUTCSeconds()).toString().padStart(2,0);

	var v_proc_context_db= PROC_CONTEXT_DB;
	var v_proc_context_schema= PROC_CONTEXT_SCHEMA;
	var v_table_name = TABLE_NAME;
	var v_src_sys_cd = SRC_SYS_CD;
	var v_bus_unit_cd = BUS_UNIT_CD;
	
	var v_error_id=0;
	var v_err_flg=Boolean(false);
	var v_proc_status_marked=Boolean(false);
	var v_delete_query_flg=Boolean(false);
	var v_proc_step = 0;
	var v_query_return_check = 0;
	var v_proc_name = ''USP_LOAD_SCDTYPE1_TBL'';
	var v_status=''FAILED'' //will be set to ''SUCCEEDED'' upon reaching last step
	var v_running_batch_flg=Boolean(false);
	
	var v_proc_status_table = v_proc_context_db+''.''+v_proc_context_schema+''.''+''PROC_EXEC_STATUS''	
	var v_error_table =  v_proc_context_db+''.''+v_proc_context_schema+''.''+''ERROR_HANDLING_LOG''
	var v_batch_cntrl_tbl= v_proc_context_db+''.''+v_proc_context_schema+''.''+''ADT_BTCH_CNTRL_LOG''
	var v_src_sys_cntrl_tbl= v_proc_context_db+''.''+v_proc_context_schema+''.''+''SRC_SYSTM_CNTRL''
	
	// To derieve the name of tmp table from target table & table is case senstive
	var v_tmp_table = v_proc_context_db+''.STAGING."TMP_'' + TABLE_NAME.replace(/"/g,"") +''"'' ;
	
    var pid_query = `SELECT DISTINCT BATCH_ID FROM `+v_batch_cntrl_tbl+` WHERE STATUS=''RUNNING'' AND LAYER_NAME = ''VAULT-`+v_src_sys_cd+`-`+v_bus_unit_cd+`'';`
	var pid_result = snowflake.execute({sqlText: pid_query});
	if (pid_result.next()) { v_running_batch_flg=Boolean(true);}
	
	//Check of the Source System is allowed to be processed
	var query_check_src_sys_cd_if_active= `SELECT MIN(IS_ENABLED) IS_ENABLED,COUNT(1) RW_CNT FROM `+v_src_sys_cntrl_tbl+` WHERE SOURCE_SYSTEM_CD=''`+v_src_sys_cd+`'' and BUS_UNIT_CD=''`+v_bus_unit_cd+`''`
	var rs_query_check_src_sys_cd_if_active = snowflake.execute({sqlText:query_check_src_sys_cd_if_active});
	
	if( v_running_batch_flg  || (rs_query_check_src_sys_cd_if_active.next() && rs_query_check_src_sys_cd_if_active.getColumnValue("IS_ENABLED") && parseInt(rs_query_check_src_sys_cd_if_active.getColumnValue("RW_CNT"))==1) )	{		}
	else if(!rs_query_check_src_sys_cd_if_active.getColumnValue("IS_ENABLED") && parseInt(rs_query_check_src_sys_cd_if_active.getColumnValue("RW_CNT"))>0){ return `FAILED: Source_System_Cd:`+v_src_sys_cd+` and BUS_UNIT_CD: `+v_bus_unit_cd+` is disabled to be processed.` }
	else if( parseInt(rs_query_check_src_sys_cd_if_active.getColumnValue("RW_CNT"))==0) {	return `FAILED: Source_System_Cd:`+v_src_sys_cd+` and BUS_UNIT_CD: `+v_bus_unit_cd+` is Inappropriate.` }

	if (v_running_batch_flg)
	{	var v_pid = pid_result.getColumnValue(1);	}
	else
	{	v_pid=''No Acitve Batch''; v_status=''Not Applicable'';
		Proc_Status_Update();
		return ''No Running Batch found, Please initiate a Batch'';	}
	
	//Check if this is intended to run or not i.e. Last execution of same PID check
	var v_status_check_pid = `SELECT NVL(MAX(CASE EXEC_STATUS WHEN ''SUCCEEDED'' THEN ''1'' WHEN ''FAILED'' THEN ''2'' END),''NULL'') FROM `+v_proc_status_table+` WHERE PROC_NAME=''`+v_proc_name+`'' and PID=''`+v_pid+`'' and TARGET_TABLE=''`+v_table_name+`'';`; 
	var v_rs_status_check_pid = snowflake.execute({sqlText:v_status_check_pid});
	
	if(v_rs_status_check_pid.next() && v_rs_status_check_pid.getColumnValue(1)==1)// All succeeded instances
	{
		return "SUCCESS: Already ran For Date[UTC]: "+v_proc_start_time+" For Target Table "+v_table_name+" and PID="+v_pid;
	}
	else if(v_rs_status_check_pid.getColumnValue(1)==2)// One failed instance found
	{
		var error_check_query = `SELECT ERROR_ID,ERROR_STEP FROM `+v_error_table+` WHERE ACTIVE_FLG=''Y'' and PROC_NAME=''`+v_proc_name+`'' and PID=''`+v_pid+`'' and TARGET_TABLE=''`+v_table_name+`'';`; 
		var v_rs_error_check = snowflake.execute({sqlText:error_check_query});
		
		if(v_rs_error_check.next())
		{
			v_error_id = v_rs_error_check.getColumnValue(1);
			v_proc_step = parseInt(v_rs_error_check.getColumnValue(2));
		}
		v_err_flg=Boolean(true);
		v_delete_query_flg=Boolean(true);
		v_proc_step = v_proc_step==0 ? 1:v_proc_step;
	}
	else { v_proc_step=1; } 


	//Query to get data from metdata
    var v_element_query =`SELECT TARGET_TBL, FINAL_QUERY, TGT_COL_LIST, DEL_TBL_ALIAS DRIVING_TABLE_ALIAS,IS_SOURCE_VAULT_TBL
							FROM `+v_proc_context_db+`.`+v_proc_context_schema+`.MDD_DYNAMIC_QUERIES
							WHERE TGT_TBL = ''`+TABLE_NAME+`'';` ;
	
	// Run the query...
	var v_element_stmt = snowflake.createStatement({sqlText:v_element_query});
	var v_element_res = v_element_stmt.execute(); 
	
    // ...And loop through the list that was returned (handles if same target has multiple queries)
	while (v_element_res.next())
	{
		v_query_return_check=1;
		var v_tgt_tbl = v_element_res.getColumnValue(1);   // Start with the element TGT_TBL
		var v_final_query =   v_element_res.getColumnValue(2);        // And finally the final select query
        var v_tgt_col_list = v_element_res.getColumnValue(3);
        var v_driving_table_alias = v_element_res.getColumnValue(4);   // driving table info for incremental load 
        var v_is_source_vault_flg = v_element_res.getColumnValue(5);   // Whether the table is being sourced from Source of Vault itself 
	
		var v_Audit_log_query=`CALL `+v_proc_context_db+`.`+v_proc_context_schema+`.USP_LOG_STATS_FOR_LAST_QUERY(''`+v_proc_context_db+`'',''`+v_proc_context_schema+`'',''`+v_pid+`'', ''`+v_proc_name+`'', ''`+v_table_name+`'', ''1'');`
		snowflake.execute({sqlText:v_Audit_log_query});
		
		v_proc_step= v_proc_step==1 ? v_proc_step+1: v_proc_step;
		
		
		if(v_is_source_vault_flg==''1'')
		{
			var v_CreatePID = `CREATE_PID`;
            var v_where = `AND `+v_driving_table_alias+`.REC_SRC_SYST_CD=''`+v_src_sys_cd+`''
							AND `+v_driving_table_alias+`.REC_SRC_BUS_UNIT_CD=''`+v_bus_unit_cd+`''
							AND `+ HKEY +` NOT IN (  SELECT `+HKEY+` FROM `+v_tgt_tbl+` ) 
                    AND `+v_driving_table_alias+`.`+v_CreatePID+` = (SELECT BATCH_ID FROM `+v_batch_cntrl_tbl+` WHERE STATUS = ''RUNNING'' AND LAYER_NAME = ''VAULT-`+v_src_sys_cd+`-`+v_bus_unit_cd+`''  );` ;
		}
		else
		{
		var v_where = ` AND `+v_driving_table_alias+`.SRC_SYS_CD=''`+v_src_sys_cd+`''
						AND `+v_driving_table_alias+`.BUS_UNIT_CD=''`+v_bus_unit_cd+`''
						AND `+ HKEY +`  NOT IN ( SELECT `+HKEY+` FROM `+v_tgt_tbl+`  )
						AND `+v_driving_table_alias+`.___REFRESH_TS_UTC >= (SELECT START_UPSERT_TS_UTC FROM `+v_batch_cntrl_tbl+`
						WHERE STATUS = ''RUNNING'' AND LAYER_NAME = ''VAULT-`+v_src_sys_cd+`-`+v_bus_unit_cd+`'') AND `+v_driving_table_alias+`.___REFRESH_TS_UTC <= (SELECT END_UPSERT_TS_UTC FROM `+v_batch_cntrl_tbl+`
						WHERE STATUS = ''RUNNING'' AND LAYER_NAME = ''VAULT-`+v_src_sys_cd+`-`+v_bus_unit_cd+`'' ) ;`
		}
		// Now build the CREATE temp table statement
		var v_tmp_ddl = `CREATE OR REPLACE TEMPORARY TABLE ` + v_tmp_table + ` AS ` + v_final_query + v_where ;
		
		var v_tmp_stmt = snowflake.createStatement({sqlText:v_tmp_ddl});
		var v_tmp_stmt_res = v_tmp_stmt.execute(); 

		var v_Audit_log_query=`CALL `+v_proc_context_db+`.`+v_proc_context_schema+`.USP_LOG_STATS_FOR_LAST_QUERY(''`+v_proc_context_db+`'',''`+v_proc_context_schema+`'',''`+v_pid+`'', ''`+v_proc_name+`'', ''`+v_table_name+`'', ''`+v_proc_step+`'');`
		snowflake.execute({sqlText:v_Audit_log_query});
		v_proc_step+= 1;

		var v_insert_ddl  = `INSERT INTO `+v_tgt_tbl+`(`+v_tgt_col_list+ `) 
							SELECT `+ v_tgt_col_list +` FROM `+v_tmp_table ;
		var v_insert_stmt = snowflake.createStatement({sqlText:v_insert_ddl});
		var v_insert_stmt_res = v_insert_stmt.execute(); 
		
		var v_Audit_log_query=`CALL `+v_proc_context_db+`.`+v_proc_context_schema+`.USP_LOG_STATS_FOR_LAST_QUERY(''`+v_proc_context_db+`'',''`+v_proc_context_schema+`'',''`+v_pid+`'', ''`+v_proc_name+`'', ''`+v_table_name+`'', ''`+v_proc_step+`'');`
		snowflake.execute({sqlText:v_Audit_log_query});
		v_proc_step+= 1;

    }
	
	if(v_query_return_check==0)
	{ 
		Proc_Status_Update()
		return ''Please check the Table name''+v_table_name+'', no row returned from MDD_DYNAMIC_QUERIES'';
	}
	
	if( v_err_flg)//Update the query to update the error record in the error table
	{
		var v_update_err_table  = `UPDATE `+v_error_table+` SET ACTIVE_FLG=''N'',UPDATE_TS_UTC=convert_timezone(''UTC'',current_timestamp())::timestamptz WHERE ERROR_ID=`+v_error_id+`;`;
		snowflake.execute({sqlText:v_update_err_table});
    }
	
	var v_status=''SUCCEEDED''
	Proc_Status_Update(); 

    var v_drop_ddl  = `DROP TABLE IF EXISTS `+v_tmp_table+` ;`;
	var v_drop_stmt = snowflake.createStatement({sqlText:v_drop_ddl});
	var v_drop_stmt_res = v_drop_stmt.execute(); 

	return "SUCCESS: For Date[UTC]: "+v_proc_start_time+" For Target Table "+v_tgt_tbl+" and PID="+v_pid;
}

catch (err) {
    result = `Failed: Step: ` + v_proc_step + `
        Code: ` + err.code + `
        State: ` + err.state;
    result += `
        Message: ` + err.message ;
    result += `
	Stack Trace:
	` + err.stackTraceTxt;
    
	err.message_detail =result;
    result=result.replace(/\\''/g,''\\''\\'''');

	if(!v_proc_status_marked)
	{
		if(v_err_flg)//Update the query to update the error record in the error table
		{
			var v_update_err_table  = `UPDATE `+v_error_table+` SET ACTIVE_FLG=''N'',UPDATE_TS_UTC=convert_timezone(''UTC'',current_timestamp())::timestamptz WHERE ERROR_ID=`+v_error_id+`;`;
			snowflake.execute({sqlText:v_update_err_table});
		}
		//update the query to insert logic for the registering the error in the table
		var insert_err_table  = `INSERT INTO `+v_error_table+` (PID, ERROR_STEP, PROC_NAME, TARGET_TABLE, ACTIVE_FLG, ERROR_MESSAGE, UPDATE_TS_UTC, LOAD_TS_UTC) VALUES (''`+v_pid+`'',''`+v_proc_step+`'',''`+v_proc_name+`'',''`+v_table_name+`'',''Y'',''`+result+`'',convert_timezone(''UTC'',current_timestamp())::timestamptz, convert_timezone(''UTC'',current_timestamp())::timestamptz);`;
		snowflake.execute({sqlText:insert_err_table});
		
		v_status=''FAILED''
		Proc_Status_Update();
	}
	else
	{
		return "Procedrure SUCCESS: For Date[UTC]: "+v_proc_start_time+" For Target Table "+v_tgt_tbl+" and PID="+v_pid+", except few commands";
	}
	// exit sproc with error message
	return "Procedrure FAILED For Date[UTC]: "+v_proc_start_time+ "\\nFor Target Table "+v_tgt_tbl+"\\nWith Error Message : " + err.message+"\\nPID="+v_pid;;
    
    }
';
