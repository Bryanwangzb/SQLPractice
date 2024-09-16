# MySQLdbのインポート
import csv
import datetime
import json
import os
import sys
import time
from logging import getLogger, config

import mysql.connector
import pandas as pd
import xlwings as xw
from mysql.connector import Error
import configparser

from openpyxl import load_workbook


with open('./log_config.json', 'r') as f:
    log_conf = json.load(f)

config.dictConfig(log_conf)

# Log Setting
logger = getLogger(__name__)

# settings initial file
inifile = configparser.ConfigParser()
inifile.read('settings.ini')

db_section = 'AWS_SLAVE'

# get db and user info from inifile.
db_host_name = inifile.get(db_section, 'db_host_name')
db_user_name = inifile.get(db_section, 'db_user_name')

# [LOCAL]Database Schema
schema_dev = inifile.get(db_section, 'schema_dev')
schema_prod = inifile.get(db_section, 'schema_prod')

# DBアカウント情報
pwd = inifile.get(db_section, 'pwd')
db = schema_prod

# Script directory
script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
# Output file directory
color_work_files = os.path.join(script_directory, "color_work_files")
os.makedirs(color_work_files, exist_ok=True)


# Connect to Database
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        logger.info("MySQL Database connection successful")
    except Error as err:
        logger.error(f"Error: '{err}'")

    return connection


# Read data through query
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        logger.error(f"Error: '{err}'")


# create connect to database
conn = create_db_connection(db_host_name, db_user_name, pwd, db)

################
# Query
################
q_color_code = f'''
SELECT
    com.id AS company_id
    , com.name AS company_name
    , 'asset' as Category
    , acl.id as color_id
    , acl.color as color_code
    , acj.name as name_ja
    , ace.name as name_en
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.asset_color_labels as acl
        ON com.id=acl.company_id
    LEFT OUTER JOIN {db}.asset_color_label_names as acj
        ON acl.id = acj.asset_color_label_id and acj.language='ja'
    LEFT OUTER JOIN {db}.asset_color_label_names as ace
        ON acl.id = ace.asset_color_label_id and ace.language='en'
WHERE
    acl.id IS NOT NULL

UNION ALL

SELECT 
    com.id AS company_id
    , com.name AS company_name
    , 'object' AS Category
    , mil.id AS color_id
    , mil.color AS color_code
    , mij.name AS name_ja
    , mie.name AS name_en
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.marker_importance_labels as mil
        ON com.id=mil.company_id 
    LEFT OUTER JOIN {db}.marker_importance_label_names as mij
        ON mil.id = mij.marker_importance_label_id and mij.language='ja'
    LEFT OUTER JOIN {db}.marker_importance_label_names as mie
        ON mil.id = mie.marker_importance_label_id and mie.language='en'      
WHERE
    mil.id IS NOT NULL
'''

q_object_color_code=f'''

'''

################
# output query result
################

# load
logger.info("Load query contents...")

# get asset color info
color_code_info = read_query(conn,q_color_code)

logger.info("Query contents are loaded.")

# Output asset color info
with open(color_work_files + '\color_code_master' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Company ID', 'Company Name', 'Category','Color ID','Color Code', 'Name Ja','Name En'])
    for _color_code_info in color_code_info:
        writer.writerow(
            [_color_code_info[0], _color_code_info[1], _color_code_info[2], _color_code_info[3],
             _color_code_info[4], _color_code_info[5],_color_code_info[6]]
        )
logger.info("asset_color_code_master csv file was created.")


