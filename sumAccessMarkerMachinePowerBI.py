# MySQLdbのインポート
import configparser
import csv
import datetime
import json
import os
import sys
import time
import boto3
from logging import getLogger, config

import mysql.connector
from mysql.connector import Error

with open('./log_config.json', 'r') as f:
    log_conf = json.load(f)

config.dictConfig(log_conf)

# Log Setting
logger = getLogger(__name__)

# settings initial file
inifile = configparser.ConfigParser()
inifile.read('settings.ini')

db_section = 'AWS_SLAVE'
aws_section = 'AWS_ACCESS'

# get db and user info from inifile.
db_host_name = inifile.get(db_section, 'db_host_name')
db_user_name = inifile.get(db_section, 'db_user_name')

# [LOCAL]Database Schema
schema_dev = inifile.get(db_section, 'schema_dev')
schema_prod = inifile.get(db_section, 'schema_prod')

# AWS access info
user_aws_access_key_id = inifile.get(aws_section, 'aws_access_key_id')
user_aws_secret_access_key = inifile.get(aws_section, 'aws_secret_access_key')
user_region_name = inifile.get(aws_section, 'region_name')
kpi_bi_bucket = inifile.get(aws_section, 'Bucket')

# DBアカウント情報
pwd = inifile.get(db_section, 'pwd')
db = schema_prod

# Script directory
script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))
# Output file directory
sum_work_files = os.path.join(script_directory, "sum_work_files")
os.makedirs(sum_work_files, exist_ok=True)

# 日付情報
running_date = datetime.date.today()
# 　日付時刻情報
timestr = time.strftime("%Y%m%d%H%M%S")

# S3クライアント
client = boto3.client(
    's3',
    aws_access_key_id=user_aws_access_key_id,
    aws_secret_access_key=user_aws_secret_access_key,
    region_name=user_region_name
)


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

# テーブルcompany_logsでの更新の最新日付を取得
log_latest_date = str(read_query(conn, q_lateste_date)[0][0])

# 日/週/月ごとに1回以上ログインしたユーザ数
q_access_amount = f'''
WITH day_access AS ( 
    SELECT
        cl.company_id
        , cl.company_user_id
        , DATE (cl.created_at) AS access_date 
    FROM
        {db}.company_logs AS cl 
        LEFT OUTER JOIN {db}.company_users AS cu 
            ON cl.company_user_id = cu.id 
            AND cl.company_id = cu.company_id 
    WHERE
        url = 'api/company/auth/me' 
        AND DATE (cl.created_at) = '{log_latest_date}' 
        AND cu.id IS NOT NULL
) 
, week_access AS ( 
    SELECT
        cl.company_id
        , cl.company_user_id
        , DATE (cl.created_at) AS access_date 
    FROM
        {db}.company_logs AS cl 
        LEFT OUTER JOIN {db}.company_users AS cu 
            ON cl.company_user_id = cu.id 
            AND cl.company_id = cu.company_id 
    WHERE
        url = 'api/company/auth/me' 
        AND DATE (cl.created_at) BETWEEN '{log_latest_date}' - INTERVAL 1 week AND '{log_latest_date}' 
        AND cu.id IS NOT NULL
) 
, month_access AS ( 
    SELECT
        cl.company_id
        , cl.company_user_id
        , DATE (cl.created_at) AS access_date 
    FROM
        {db}.company_logs AS cl 
        LEFT OUTER JOIN {db}.company_users AS cu 
            ON cl.company_user_id = cu.id 
            AND cl.company_id = cu.company_id 
    WHERE
        url = 'api/company/auth/me' 
        AND DATE (cl.created_at) BETWEEN '{log_latest_date}' - INTERVAL 1 MONTH AND '{log_latest_date}' 
        AND cu.id IS NOT NULL
) 
, month_login_user_amount AS ( 
    SELECT
        company_id
        , company_user_id 
    FROM
        month_access 
    GROUP BY
        company_id
        , company_user_id
) 
, week_login_user_amount AS ( 
    SELECT
        company_id
        , company_user_id 
    FROM
        week_access 
    GROUP BY
        company_id
        , company_user_id
) 
, day_login_user_amount AS ( 
    SELECT
        company_id
        , company_user_id 
    FROM
        day_access 
    GROUP BY
        company_id
        , company_user_id
) 
, markers_amount AS ( 
    SELECT
        plant_area_id
        , count(DISTINCT id) AS marker_amount 
    FROM
        {db}.markers 
    GROUP BY
        plant_area_id
) 
, machines_amount AS ( 
    SELECT
        plant_area_id
        , count(DISTINCT id) AS machine_amount 
    FROM
        {db}.assets 
    GROUP BY
        plant_area_id
) 
, user_amount AS ( 
    SELECT
        company_id
        , count(id) AS user_amount 
    FROM
        {db}.company_users 
    GROUP BY
        company_id
) 
, measure_amount AS ( 
    SELECT
        plant_area_id
        , count(plant_area_id) AS measure_amount 
    FROM
        {db}.measure_lengths 
    GROUP BY
        plant_area_id
) 
, simulation_amount AS ( 
    SELECT
        plant_area_id
        , count(plant_area_id) AS simulation_amount 
    FROM
        {db}.plant_area_objects 
    GROUP BY
        plant_area_id
) 
SELECT
    com.id AS company_id
    , com.name AS company_name
    , pa.id AS area_id
    , pa.name AS area_name
    , count(DISTINCT dlu.company_user_id) AS day_count
    , round( 
        ( 
            count(DISTINCT dlu.company_user_id) / ua.user_amount
        ) * 100
        , 1
    ) AS day_user_ratio
    , count(DISTINCT wlu.company_user_id) AS week_count
    , round( 
        ( 
            count(DISTINCT wlu.company_user_id) / ua.user_amount
        ) * 100
        , 1
    ) AS week_user_ratio
    , count(DISTINCT mlu.company_user_id) AS month_count
    , round( 
        ( 
            count(DISTINCT mlu.company_user_id) / ua.user_amount
        ) * 100
        , 1
    ) AS month_user_ratio
    , mka.marker_amount AS marker_amount
    , mca.machine_amount AS machine_amount
    , ma.measure_amount AS measure_amount
    , sa.simulation_amount AS simulation_amount 
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN {db}.plant_areas AS pa 
        ON pla.id = pa.plant_id 
    LEFT OUTER JOIN day_login_user_amount AS dlu 
        ON com.id = dlu.company_id 
    LEFT OUTER JOIN user_amount AS ua 
        ON com.id = ua.company_id 
    LEFT OUTER JOIN week_login_user_amount AS wlu 
        ON com.id = wlu.company_id 
    LEFT OUTER JOIN month_login_user_amount AS mlu 
        ON com.id = mlu.company_id 
    LEFT OUTER JOIN markers_amount AS mka 
        ON pa.id = mka.plant_area_id 
    LEFT OUTER JOIN machines_amount AS mca 
        ON pa.id = mca.plant_area_id 
    LEFT OUTER JOIN measure_amount AS ma 
        ON pa.id = ma.plant_area_id 
    LEFT OUTER JOIN simulation_amount AS sa 
        ON pa.id = sa.plant_area_id 
WHERE
    pa.id IS NOT NULL
GROUP BY
    com.id
    , com.name
    , pla.id
    , pla.name
    , pa.id
    , pa.name
'''

# latest date from company logs
q_company_latest_date = f'''
SELECT
    DATE (max(created_at)) 
FROM
    {db}.company_logs
'''

# latest date from marker
q_marker_latest_date = f'''
SELECT
    DATE (max(created_at)) 
FROM
    {db}.markers
'''

# latest date from asset
q_machine_latest_date = f'''
SELECT
    DATE (max(created_at)) 
FROM
    {db}.assets
'''

# latest date from measurement
q_measurement_latest_date = f'''
SELECT
    DATE (max(created_at))
FROM
    {db}.measure_lengths
'''

# latest date from spatial simulation
q_simulation_latest_date = f'''
SELECT 
    DATE (max(created_at))
FROM
    {db}.plant_area_objects
'''



conn = create_db_connection(db_host_name, db_user_name, pwd, db)

# load
logger.info("Load query contents...")
results = read_query(conn, q_access_amount)

# get company log latest date
company_log_latest_date = read_query(conn, q_company_latest_date)
# get marker latest date
marker_latest_date = read_query(conn, q_marker_latest_date)
# get machine latest date
machine_latest_date = read_query(conn, q_machine_latest_date)
# get measurement latest date
measurement_latest_date = read_query(conn, q_measurement_latest_date)
# get simulation latest date
simulation_latest_date = read_query(conn, q_simulation_latest_date)

logger.info("Query contents are loaded.")

# add suffix to users,ratio,amount values.
get_user = lambda x: str.strip(str(x)) + " users"
get_ratio = lambda x: str(x or '0.0') + "%"
get_amount = lambda x: str(x or '0') + " pcs"

with open(sum_work_files + '\\pb_access_marker_machine_amounts' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Update Date', 'Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name', 'Day Access Amount (Ratio)',
         'Week Access Amount (Ratio)', 'Month Access Amount (Ratio)',
         'Marker Total Registered', 'Machine Total Registered', 'Measurement Total Registered',
         'Simulation Total Registered',
         'company_logs_UpdateDate', 'markers_UpdateDate',
         'assets_UpdateDate', 'measurements_UpdateDate', 'simulation_UpdateDate', 'Data_LatestDate'])
    for result in results:
        writer.writerow(
            [running_date, result[0], result[1], result[2], result[3],
             get_user(result[4]) + " (" + get_ratio(result[5]) + ")",
             get_user(result[6]) + " (" + get_ratio(result[7]) + ")"
                , get_user(result[8]) + " (" + get_ratio(result[9]) + ")", get_amount(result[10]),
             get_amount(result[11]), get_amount(result[12]), get_amount(result[13]), company_log_latest_date[0]
             [0], marker_latest_date[0][0], machine_latest_date[0][0], measurement_latest_date[0][0],
             simulation_latest_date[0][0],
             max(marker_latest_date[0][0], machine_latest_date[0][0], measurement_latest_date[0][0],
                 simulation_latest_date[0][0])])
logger.info("access_marker_machine_contents csv file is created.")

# company, plant area ID and name
with open(sum_work_files + '\\company_area_master' + '.csv','w',newline='',encoding='utf-8') as  file:
    writer = csv.writer(file)
    writer.writerow(
        ['Company ID','Company Name','Plant Area ID','Plant Area Name']
    )
    for result in results:
        writer.writerow(
            [result[0],result[1],result[2],result[3]]
        )
logger.info("company_area_master csv file is created.")

# Update BI workbook's weekly transaction sheet.
logger.info("Updating BI file...")

# BI_workbook.save()
# BI_workbook.close()
logger.info("BI file updating is completed.")

# Add company_area_master.csv file here

# Upload csv file to AWS S3 bucket
logger.info("Upload file to AWS S3 bucket...")
Filename = './sum_work_files/pb_access_marker_machine_amounts.csv'
Bucket = kpi_bi_bucket
Key = 'pb_access_marker_machine_amounts.csv'
client.upload_file(Filename, Bucket, Key)
logger.info('CSV file is uploaded.')
# 接続を閉じる
conn.close()
