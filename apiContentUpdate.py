import pandas as pd
# MySQLdbのインポート
import csv
import datetime
import json
import logging
import os
import sys
import time
from logging import getLogger, config

import mysql.connector
import pandas as pd
import xlwings as xw
from mysql.connector import Error
import configparser

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
user_transaction_files = os.path.join(script_directory, "user_transaction_files")
os.makedirs(user_transaction_files, exist_ok=True)

# 日付情報
running_date = datetime.date.today()
# 　日付時刻情報
timestr = time.strftime("%Y%m%d%H%M%S")

# 取得データの日付範囲
start_date = '2024/08/01'
end_date = '2024/08/31'

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


# create connect to database
conn = create_db_connection(db_host_name, db_user_name, pwd, db)

################
# クエリ
################
q_lateste_date = f'''
SELECT
    MAX(DATE(created_at)) 
FROM
    {db}.company_logs
'''
latest_date = str(read_query(conn, q_lateste_date)[0][0])

q_company_log = f'''
WITH api_url_log AS ( 
    SELECT
        cl.company_id AS company_id
        , com.name AS company_name
        , cl.plant_id AS plant_id
        , pla.name AS plant_name
        , cl.plant_area_id AS plant_area_id
        , pa.name AS area_name
        , cl.company_user_id AS company_user_id
        , cu.name AS company_user_name
        , CASE 
            WHEN cl.platform = 1 
                THEN 'web' 
            WHEN cl.platform = 2 
                THEN 'app' 
            END AS platform
        , cl.url AS url
        , CASE 
            WHEN json_extract(request_parameters, '$.plant_id') IS NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NULL 
                THEN request_parameters 
            WHEN json_extract(request_parameters, '$.plant_id') IS NOT NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NULL 
                THEN concat('プラント名: ', COALESCE(pla.name, '未取得')) 
            WHEN json_extract(request_parameters, '$.plant_id') IS NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NULL 
                THEN request_parameters 
            WHEN json_extract(request_parameters, '$.plant_id') IS NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NOT NULL 
                THEN concat('エリア名: ', COALESCE(pa.name, '未取得')) 
            END AS request_paramteres_updated
        ,  DATE_SUB(cl.created_at, INTERVAL 9 HOUR)  as created_at
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN companies AS com 
            ON cl.company_id = com.id 
        LEFT OUTER JOIN plants AS pla 
            ON cl.plant_id = pla.id 
            AND json_extract(cl.request_parameters, '$.plant_id') = pla.id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON cl.plant_area_id = pa.id 
            AND json_extract(cl.request_parameters, '$.plant_area_id') = pa.id 
        LEFT OUTER JOIN company_users AS cu 
            ON cl.company_user_id = cu.id 
    WHERE
        DATE (date_sub(cl.created_at,interval 9 hour)) BETWEEN '{start_date}' AND '{end_date}' 
        AND cl.company_id = '82' 
        AND cu.name not like '%BRS%'
    UNION 
    SELECT
        cu.company_id AS company_id
        , com.name AS company_name
        , cl.plant_id AS plant_id
        , pla.name AS plant_name
        , cl.plant_area_id AS plant_area_id
        , pa.name AS area_name
        , cl.company_user_id AS company_user_id
        , cu.name AS company_user_name
        , CASE 
            WHEN cl.platform = 1 
                THEN 'web' 
            WHEN cl.platform = 2 
                THEN 'app' 
            END AS platform
        , cl.url AS url
        , CASE 
            WHEN json_extract(request_parameters, '$.plant_id') IS NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NULL 
                THEN request_parameters 
            WHEN json_extract(request_parameters, '$.plant_id') IS NOT NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NULL 
                THEN concat('プラント名: ', COALESCE(pla.name, '未取得')) 
            WHEN json_extract(request_parameters, '$.plant_id') IS NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NULL 
                THEN request_parameters 
            WHEN json_extract(request_parameters, '$.plant_id') IS NULL 
            AND json_extract(request_parameters, '$.plant_area_id') IS NOT NULL 
                THEN concat('エリア名: ', COALESCE(pa.name, '未取得')) 
            END AS request_paramteres_updated
        ,  DATE_SUB(cl.created_at, INTERVAL 9 HOUR) as created_at
    FROM
        company_users AS cu 
        LEFT OUTER JOIN companies AS com 
            ON cu.company_id = com.id 
        LEFT OUTER JOIN company_logs AS cl 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
        LEFT OUTER JOIN plants AS pla 
            ON cl.plant_id = pla.id 
            AND json_extract(cl.request_parameters, '$.plant_id') = pla.id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON cl.plant_area_id = pa.id 
            AND json_extract(cl.request_parameters, '$.plant_area_id') = pa.id 
    WHERE
        DATE (date_sub(cl.created_at,interval 9 hour)) BETWEEN '{start_date}' AND '{end_date}' 
        AND cl.url = 'api/company/auth/login' 
        AND cu.company_id = '82'
        AND cu.name not like '%BRS%'
) 
SELECT
    * 
FROM
    api_url_log 
ORDER BY 
    created_at
'''
# WHERE DATE(cl.created_at) BETWEEN DATE_SUB('{latest_date}',INTERVAL 1 MONTH) AND '{latest_date}'
# WHERE DATE(cl.created_at) BETWEEN '2024/4/1' AND '2024/5/14' AND cl.company_id='171'
# change the where part back to following in the future
# DATE(cl.created_at) BETWEEN DATE_SUB('{latest_date}',INTERVAL 1 MONTH) AND '{latest_date}'
# for work : DATE(cl.created_at) BETWEEN '2024/03/11' AND '2024/05/07' and com.name='AGC鹿島工場'

################
# 実行結果
################

with conn.cursor() as cursor:
    cursor.execute(q_company_log)
    df_logs = pd.DataFrame(cursor.fetchall())
# 出力用
df_logs.columns = ['company_id','company_name','plant_id','plant_name','plant_area_id','plant_area_name','company_user_id','company_user_name','platform','url','request_paramteres_updated','created_at']


df_api = pd.read_excel('API_Details.xlsx',index_col=0)

merged_df = pd.merge(df_logs,df_api,on='url',how='left')

merged_df['url'].fillna(merged_df['Operation Details'],inplace=True)

merged_df.to_csv('C:\\Users\WorkAccount\Box\\api_url_log_data.csv', index=False,encoding="utf-8")

merged_df.drop(columns=['url'],inplace=True)

merged_df.rename(columns={'url':'Operation Details'},inplace=True)
merged_df = merged_df[['company_id','company_name','plant_id','plant_name','plant_area_id','plant_area_name','company_user_id','company_user_name','platform','Operation Details','request_paramteres_updated','created_at']]
merged_df.to_csv('C:\\Users\WorkAccount\Box\\api_log_data.csv', index=False,encoding="utf-8")


logger.info("API updated file csv was created.")