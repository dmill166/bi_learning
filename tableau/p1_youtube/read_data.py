import os
import pandas
import json
import pyodbc
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.types import NVARCHAR
QUOTECHAR = '"'
ENCODING = 'utf-8'
DEBUG = True
SERVER_STR = 'p1youtube_server'
DATABASE_STR = 'p1youtube_database'
USER_STR = 'p1youtube_user'
PASS_STR = 'p1youtube_pass'
BATCH_INSERT_SIZE = 50000
DATA_SUBFOLDER = '.\\tableau\p1_youtube\data\\'

# Resources:
'''
R00. https://dakotalearns.wordpress.com/2022/11/26/virtual-environments-what-why-how/#summary-and-links
R01. https://pandas.pydata.org/docs/user_guide/io.html
R02. https://stackoverflow.com/questions/5552555/unicodedecodeerror-invalid-continuation-byte # fixed encoding issue
R03. https://stackoverflow.com/questions/19828822/how-to-check-whether-a-pandas-dataframe-is-empty # checking empty df
R04. https://pandas.pydata.org/docs/reference/api/pandas.concat.html # use of concat function
R05. https://www.geeksforgeeks.org/add-column-with-constant-value-to-pandas-dataframe/ # adding constant column to df
R06. https://stackoverflow.com/questions/64037243/pythonhow-to-split-column-into-multiple-columns-in-a-dataframe-and-with-dynamic # split nested JSON
R07. https://www.nylas.com/blog/making-use-of-environment-variables-in-python/ # environment variables in Python
R08. https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=sql-server-ver16 #insert data into mssql using pandas df
R09. https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html # pandas to_Sql docs
R10. https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc # sqlalchemy mssql docs
R11. https://stackoverflow.com/questions/55801668/the-type-of-field-is-not-a-sqlalchemy-type-with-pandas-to-sql-to-an-oracle-dat #df datatypes for sql insert
'''

def read_csv(file_path, quotechar, encoding, debug_mode = False):
    """Reads a CSV file based on file path, quote character, file encoding, and potential debug messaging.
    Returns DataFrame of CSV object."""

    if debug_mode:
        print("File Path: {0}".format(file_path))
    csv = pandas.read_csv(file_path,quotechar=quotechar,encoding=encoding)
    if debug_mode:
        print("File path {0} read successfully.".format(file_path))
    return csv


def read_json(file_path, debug_mode = False):
    """Reads a JSON file based on file path and potential debug messaging.
    Returns DataFrame of JSON object."""
    if debug_mode:
        print("File Path: {0}".format(file_path))
    json = pandas.read_json(file_path)
    d = [pandas.DataFrame(json[col].tolist()).add_prefix(col) for col in json.columns]
    json = pandas.concat(d, axis=1)
    if debug_mode:
        print("File path {0} read successfully.".format(file_path))
    return json


def read_data(debug_mode = False):
    """Facilitates obtaining of CSV and JSON files from an underlying data folder.
    Returns two DataFrame objects in a single list:
        one concatenation of all CSV DataFrames and one concatenation of all JSON DataFrames."""
    # Obtain current working directory
    if debug_mode:
        print("Obtaining CWD...")
    cwd = os.getcwd()
    if debug_mode:
        print('CWD: {0}'.format(cwd))
        print("CWD Obtained.\n")

    # Drill down into data directory
    if debug_mode:
        print("Changing into data subfolder...")
    os.chdir('.\\tableau\p1_youtube\data\\')
    if debug_mode:
        print('CWD: {0}'.format(os.getcwd()))
        print("Successful change into data subfolder.\n")

    # Collect list of file names with pattern matching
    if debug_mode:
        print("Gathering csv file paths...")
    csv_paths = []
    for x in os.listdir():
        if x.endswith(".csv"):
        # Appends only csv file present in folder
            csv_paths.append(cwd + DATA_SUBFOLDER + x)
    if debug_mode:
        print('CSV Paths: {0}: '.format(csv_paths))
        print("CSV File Paths gathered.\n")

    if debug_mode:
        print("Gathering json file paths...")
    json_paths = []
    for x in os.listdir():
        if x.endswith(".json"):
        # Appends only csv file present in folder
            json_paths.append(cwd + DATA_SUBFOLDER + x)
    if debug_mode:
        print('JSON Paths: {0}: '.format(json_paths))
        print("JSON File Paths gathered.\n")

    # Iterate through file paths and append results to main dataframes (adding file_name as a column)
    if debug_mode:
        print("Reading CSV files...")
    csv_df = pandas.DataFrame()
    for file_path in csv_paths:
        the_file_name = file_path.split("data\\",1)[1]
        if csv_df.empty:
            csv_df = read_csv(file_path, quotechar=QUOTECHAR,encoding=ENCODING, debug_mode=DEBUG)
            csv_df['file_name'] = pandas.Series([the_file_name for x in range(len(csv_df.index))])
        else:
            temp_df = read_csv(file_path, quotechar=QUOTECHAR,encoding=ENCODING, debug_mode=DEBUG)
            temp_df['file_name'] = pandas.Series([the_file_name for x in range(len(temp_df.index))])
            csv_df = pandas.concat([csv_df, temp_df])
    csv_df.name = 'csv_df'
    if debug_mode:
        print(csv_df)
        print("CSV Files read.\n")

    if debug_mode:
        print("Reading JSON files...")
    json_df = pandas.DataFrame()
    for file_path in json_paths:
        the_file_name = file_path.split("data\\",1)[1]
        if json_df.empty:
            json_df = read_json(file_path, debug_mode=DEBUG)
            json_df['file_name'] = pandas.Series([the_file_name for x in range(len(json_df.index))])
        else:
            temp_df = read_json(file_path, debug_mode=DEBUG)
            temp_df['file_name'] = pandas.Series([the_file_name for x in range(len(temp_df.index))])
            json_df = pandas.concat([json_df, temp_df])
    json_df.name = 'json_df'
    if debug_mode:
        print(json_df)
        print("JSON Files read.\n")

    list_of_df = [csv_df, json_df]
    return list_of_df

def sql_driver(list_of_df, debug_mode=DEBUG):
    """Iterates through a list of dataframes and inserts them into staging tables within SQL Server"""
    # obtain creds
    if debug_mode:
        print("Obtaining credentials...")
    prj_server = os.environ.get(SERVER_STR)
    prj_db = os.environ.get(DATABASE_STR)
    prj_user = os.environ.get(USER_STR)
    prj_pass = os.environ.get(PASS_STR)
    if debug_mode:
        print("Credentials obtained!")

    # connect to database
    if debug_mode:
        print("Attempting connection...")
    connection_url = f"mssql+pyodbc://{prj_user}:{prj_pass}@{prj_server}/{prj_db}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_url, fast_executemany=True)

    if debug_mode:
        print("Connection successful!")

    # insert dataframes
    list_of_df.pop(1)
    for df in list_of_df:

        df.to_sql(df.name, engine, schema='dbo', if_exists='append',
         chunksize=BATCH_INSERT_SIZE,index=None)

    engine.dispose()
             

def main():
    list_of_df = read_data(debug_mode=DEBUG)
    sql_driver(list_of_df, debug_mode=DEBUG)


if __name__ == '__main__':
    main()