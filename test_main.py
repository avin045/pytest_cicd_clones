from snowflake.snowpark import Session
from snowflake.snowpark.dataframe import col
import logging
import os
# pytest -v -s pytest_cicd.py
# pytest -o log_cli=true -v -s pytest_cicd.py
# pytest -o log_cli=true -v -s pytest_cicd.py
# pytest --log-file=test_log.txt test_example.py


# Create and configure logger
def setup_logger():
    with open("./log_/pytest.log", 'w'):
        pass  # 'pass' is a no-op statement that does nothing

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)  # Set the desired log level (INFO, DEBUG, ERROR, etc.)

    # Create a log file handler to write log messages to a file
    log_file = './log_/pytest.log'
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)  # Set log level for the file handler

    # Create a console handler to print log messages to the console
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.INFO)  # Set log level for the console handler

    # Create a formatter to define the format of log messages
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    # console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    # logger.addHandler(console_handler)

    return logger

logger = setup_logger()

user = os.environ.get("snow_user")
password = os.environ.get("snow_pwd")
account = os.environ.get("snow_acc")
warehouse='COMPUTE_WH'
role = 'ACCOUNTADMIN'


connection_parameters = {
    "user": user,
    "password" : password,
    "account": account,
    "role": role,
    "warehouse": warehouse
 }

session = Session.builder.configs(connection_parameters).create()

session.use_database(database='HR_DB');
session.use_schema(schema='HR_SCHEMA');
session.use_warehouse(warehouse='COMPUTE_WH');

file_path = r'./transformation_query/query.sql';
file_content = open(file=file_path,mode='r+');
query = file_content.read();
# print(query)

# '''
# source & target CHECK
src = session.sql(query=query.replace(';',''));
target = session.table(name='HR_SCHEMA.EMPLOYEE_DETAILS_DYNAMIC');

# INPUT for Getting COLUMNS LIST
columns_list = input("Enter the column list with ',' seperated : ").split(',')
# columns_list = "EMPID,DEPARTMENT,POSITION".split(',')
list_of_cols = [col_.strip() for col_ in columns_list]

def test_rowcount():
    logger.info(f"ROWCOUNT => The count of Source : {src.count()} and Target : {target.count()} Matching : {src.count() == target.count()}")
    assert src.count() == target.count()

def test_data_mismatch():
    mismatch_cols = []
    matched_cols = [tgt_col for src_col,tgt_col in zip(sorted(src.columns),sorted(target.columns)) if src_col == tgt_col]
    result = src.select(matched_cols).minus(target.select(matched_cols)).collect()
    log_res = 'PASSED' if not bool(result) == True else 'NOT PASSED'
    logger.info(f"DATA MISMATCH : {log_res}")
    assert not bool(result) == True,f"The Mismatching count is {len(mismatch_cols)} \n and the columns are {','.join(mismatch_cols)}"

def test_duplicates_or_not():
    # EMPLOYEE_NUMBER, EMPLOYEE_FULL_NAME, EMPLOYEE_STATUS_CODE, DEPARTMENT_NUMBER, SUPERVISOR_NUMBER
    # EMPLOYEE_NUMBER, EMPLOYEE_FULL_NAME, EMPLOYEE_STATUS_CODE, DEPARTMENT_NUMBER # current one
    # EMPID,DEPARTMENT,POSITION
    result = target.select(list_of_cols).group_by(list_of_cols).count().filter(col('count') > 1).collect() # []
    log_res = 'PASSED' if not bool(result) == True else 'NOT PASSED'
    logger.info(f"DUPLICATES OR NOT : {log_res}")
    assert not bool(result) == True

def test_null_check():
    null_columns = ','.join([
    col_ for col_ in target.select(list_of_cols).columns if target.select(list_of_cols).where(target.select(list_of_cols).col(col_).is_null()).collect()
    ])
    log_res = 'PASSED' if target.count() == target.select(list_of_cols).dropna().count() else 'NOT PASSED'
    logger.info(f"NULL CHECK : {log_res}")
    assert True if target.count() == target.select(list_of_cols).dropna().count() else False, f"The Null Columns are {null_columns}"

# '''