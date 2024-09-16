import csv
import datetime
import json
import logging
import os
import sys
import time
from logging import getLogger, config

import mysql.connector
import numpy as np
import pandas as pd
import xlwings as xw
from mysql.connector import Error
import configparser

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


# データベースに接続
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


# データの読み取り
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        logger.error(f"Error: '{err}'")


# DBに接続
conn = create_db_connection(db_host_name, db_user_name, pwd, db)

# IVR Company情報取得用のクエリ
q_ivr_company_info = f'''
SELECT
    id, name
FROM
    {db}.companies
'''

#　クエリ実行
ivr_company_infos = read_query(conn, q_ivr_company_info)

# IVR Company情報をCSVに出力
with open( 'IVR_Companies_Query' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['id', 'name'])
    for ivr_company_info in ivr_company_infos:
        writer.writerow(
            [ivr_company_info[0], ivr_company_info[1]]
        )
# Boxに結果を出力
# Wait until box can be accessed
# with open( 'C:\\Users\WorkAccount\Box\IVR_Companies_Query' + '.csv', 'w', newline='', encoding='utf-8') as file:
#     writer = csv.writer(file)
#     writer.writerow(['id', 'name'])
#     for ivr_company_info in ivr_company_infos:
#         writer.writerow(
#             [ivr_company_info[0], ivr_company_info[1]]
#         )

