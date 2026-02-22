import pandas as pd
import pytest
import pytest_check as check
import pyodbc
import oracledb
import logging
from sqlalchemy import create_engine

#logging confing
logging.basicConfig(
    filename = "Logs/EtlLogFile.log",
    filemode = "a",
    format = "%(asctime)s-%(levelname)s-%(message)s",
    level = logging.INFO
)
logger = logging.getLogger(__name__)

mssql_conn = ("mssql+pyodbc://LAPTOP-R3E5KI54\SQLEXPRESS/ETL_Automation_Project?"
                  "driver=ODBC+Driver+17+for+SQL+Server")
eng_conn = create_engine(mssql_conn)

class TestDataValidation:

    @pytest.mark.skip
    def test_table_exist_validation_in_DB(self,mssql_db_connection):
        try:
            expected_table_name = ['Stagging_inventory','Stagging_sales']
            actual_query = """select TABLE_NAME from information_schema.tables where TABLE_NAME in ('Stagging_inventory','Stagging_sales')"""
            df_query = pd.read_sql(actual_query,mssql_db_connection)
            actual_table_list = df_query['TABLE_NAME'].to_list()
            missing_table_name = []
            for table in expected_table_name:
                if table not in actual_table_list:
                    missing_table_name.append(table)
            assert len(missing_table_name) == 0,f"Table name {missing_table_name} does not exist"
        except Exception as e:
            logger.error("Please check Table name")
            pytest.fail("Check code error")

    @pytest.mark.skip
    def test_number_of_column_validation_Inter_filtered_sales(self,mssql_db_connection):
        try:
            expected_count = 7
            tgt_query = """select * from Inter_filtered_sales"""
            df_tgt_result = pd.read_sql(tgt_query,mssql_db_connection)
            tgt_col_count = df_tgt_result.shape[1]
            assert expected_count == tgt_col_count,("Count of Column is not matching")
        except Exception as e:
            logger.error("Code is not working")
            pytest.fail("Please check your program")

    @pytest.mark.skip
    def test_column_name_validation_filter_table(self,mssql_db_connection):
        try:
            expected_col_name = ['sales_id','product_id','store_id','quantity','price','sale_date','region']
            tgt_query = """select * from Inter_filtered_sales"""
            df_col_result = pd.read_sql(tgt_query,mssql_db_connection)
            actual_col_name = list(df_col_result.columns)
            missing_col_name = []
            for colname in expected_col_name:
                if colname not in actual_col_name:
                    missing_col_name.append(colname)
            assert len(missing_col_name) ==0 ,"Column name is not matching"
        except Exception as e:
            logger.error(f"Column name {missing_col_name} is not available",e,exc_info=True)
            pytest.fail("Code error")

    @pytest.mark.skip
    def test_count_validation_filtere_sales(self,mssql_db_connection):
        logger.info("Count validation started..")
        try:
            src_query = """select * from stagging_sales where sale_date >= '2024-09-10'"""
            df_actual = pd.read_sql(src_query,mssql_db_connection)
            tgt_query = """select * from Inter_filtered_sales"""
            df_expected = pd.read_sql(tgt_query,mssql_db_connection)
            assert len(df_actual) == len(df_expected),"Count is not matching"
            logger.info(" Count validation completed..")
        except Exception as e:
            logger.error("Count validation Failed.. ")
            pytest.fail("Error in Code")

    @pytest.mark.skip
    def test_count_validation_filter_sales_table(self,mssql_db_connection):
        try:
            src_query = """select * from stagging_sales where sale_date >= '2024-09-10'"""
            df_actual = pd.read_sql(src_query,mssql_db_connection)
            tgt_query = """select * from Inter_filtered_sales"""
            df_expected = pd.read_sql(tgt_query,mssql_db_connection)

            assert len(df_actual) == len(df_expected),"Count is not matching"
        except Exception as e:
            logger.error("Code has failed",e,exc_info = True)
            pytest.fail("Code error")

    @pytest.mark.skip
    def test_HR_duplicate_record_validation(self,mssql_HR_db_connection):
        try:
            tgt_query = """select * from duplicate_rec"""
            df_duplicate_rec = pd.read_sql(tgt_query,mssql_HR_db_connection)
            dup_result = df_duplicate_rec[df_duplicate_rec.duplicated()]
            dup_result.to_csv("HR_Duplicate_rec.csv",index=False)

            dup_rec_count = df_duplicate_rec.duplicated().sum()
            assert dup_rec_count == 0,"Duplicate record is available"
        except Exception as e:
            logger.error("There are duplicate record is available in the Target table.",e,exc_info=True)
            pytest.fail("Program has failed.")

    def test_Null_record_validation(self,mssql_HR_db_connection):
        try:
            logger.info("Null record validation has started.")
            tgt_query = """select * from departments"""
            df_null_result = pd.read_sql(tgt_query,mssql_HR_db_connection)
            null_columns = df_null_result.columns[df_null_result.isnull().any()]
            assert len(null_columns) == 0,f"NULL values found in columns: {list(null_columns)}"
            logger.info("Null record validation has completed.")
        except Exception as e:
            logger.error("NULL values found in employees table.",e,exc_info=True)
            pytest.fail("Please check the code")

    @pytest.mark.skip
    def test_data_validation_minus_query_filter_sales_table(self,mssql_db_connection):
        try:
            src_query = """select * from stagging_sales where sale_date >= '2024-09-10'"""
            df_src = pd.read_sql(src_query,mssql_db_connection)
            tgt_query = """select * from Inter_filtered_sales"""
            df_tgt = pd.read_sql(tgt_query,mssql_db_connection)

            #src minus target
            src_extra_rec = df_src[~df_src.apply(tuple,axis=1).isin(df_tgt.apply(tuple,axis=1))]

            #tgt minus source
            tgt_extra_rec = df_tgt[~df_tgt.apply(tuple,axis=1).isin(df_src.apply(tuple,axis=1))]

            assert src_extra_rec.empty,"Extra are available at Source table "
            assert tgt_extra_rec.empty,"Extra are available at Target table"
        except Exception as e:
            logger.error("Code error",e,exc_info=True)
            pytest.fail("Data validation program has failed.")

    @pytest.mark.skip
    def test_agg_transformation_data_validation_filter_sales(self,mssql_db_connection):
        try:
            src_query = """select product_id, year(sale_date) as year, month(sale_date) as month, sum(quantity*price) as total_sales
                                from Inter_filtered_sales
                                group by product_id, year(sale_date), month(sale_date)"""
            df_src = pd.read_sql(src_query,mssql_db_connection)
            tgt_query = """select * from Inter_Monthly_Sales_Summary"""
            df_tgt = pd.read_sql(tgt_query,mssql_db_connection)

            assert df_src.equals(df_tgt),"Agg transformation is not working."
        except Exception as e:
            logger.error("Agg Transformation has failed.",e,exc_info=True)
            pytest.fail("Agg Transformation code error.")

    def test_join_transformation_validation_sales_with_Details_table(self,mssql_db_connection):
        #logger.info("Join transformation validation has started.")
        try:
            src_query = """select store_id, sum(quantity_on_hand) as total_inventory
                                    from stagging_inventory group by store_id"""
            df_src_result = pd.read_sql(src_query,mssql_db_connection)
            tgt_query = """select * from Inter_aggregated_inventory_level"""
            df_tgt_result = pd.read_sql(tgt_query,mssql_db_connection)
            assert df_src_result.equals(df_tgt_result),"Not matching"
            #logger.info("Join transformation validation has completed.")
        except Exception as e:
            logger.error("Join Transformation failed",e,exc_info=True)
            pytest.fail("Please check code.")

