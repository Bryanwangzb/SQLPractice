import configparser
import csv
import datetime
import json
import os
import sys
import boto3
from datetime import datetime
from logging import getLogger, config

import mysql.connector
import pandas as pd
import xlwings as xw
from dateutil.relativedelta import relativedelta
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
weekly_work_files = os.path.join(script_directory, "weekly_work_files")
os.makedirs(weekly_work_files, exist_ok=True)

# S3 Client
client = boto3.client(
    's3',
    aws_access_key_id=user_aws_access_key_id,
    aws_secret_access_key=user_aws_secret_access_key,
    region_name=user_region_name
)

# Date information
# Temporary using temp_running_date for developing, but need change to running_date in the productive environment
# running_date = datetime.now().date()
# get the latest date from company_log
# following for productive
# minus_one_year = running_date - relativedelta(years=1)

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
q_lateste_date = f'''
SELECT
    MAX(DATE(created_at)) 
FROM
    {db}.company_logs
'''

# Comapny and area info
q_company_area_id = f'''
SELECT
    com.id AS company_id
    , com.name as company_name
    , pa.id AS area_id 
    , pa.name as area_name
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN {db}.plant_areas AS pa 
        ON pla.id = pa.plant_id 
WHERE
    pla.id IS NOT NULL 
    AND pa.id IS NOT NULL
'''
# get latest data
running_date = str(read_query(conn, q_lateste_date)[0][0])
minus_one_year = datetime.strptime(running_date, '%Y-%m-%d').date() - relativedelta(years=1)

# User weekly sum of login times through one year depends on Company ID
q_user_weekly_change = f'''
WITH year_access AS (
    SELECT
        company_id,
        company_user_id,
        DATE(created_at) AS access_date
    FROM
        {db}.company_logs
    WHERE
        url = 'api/company/auth/me'
        AND DATE(created_at) BETWEEN DATE_SUB('{running_date}', INTERVAL 1 YEAR) AND '{running_date}'
),

year_change_week AS (
    SELECT
        company_id,
        company_user_id,
        yearweek(access_date) AS year_week
    FROM
        year_access
    GROUP BY
        company_id,
        company_user_id,
        yearweek(access_date)
),

year_user_num AS (
    SELECT
        company_id,
        year_week,
        company_user_id
    FROM
        year_change_week
    GROUP BY
        company_id,
        year_week,
        company_user_id
)

SELECT
    company_id,
    year_week,
    COUNT(company_user_id) AS weekly_amount
FROM
    year_user_num
GROUP BY
    company_id,
    year_week
ORDER BY
    company_id;
'''

# 　marker week sum through one year based on area ID
q_marker_weekly_change = f'''
SELECT
    plant_area_id
    , 
    yearweek(created_at)year_week
    , count(plant_area_id) AS marker_weekly 
FROM
    {db}.markers 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , 
    yearweek(created_at)
  
ORDER BY
    plant_area_id

'''

# 　machine week sum through one year based on area ID
q_machine_weekly_change = f'''
SELECT
    plant_area_id
    , 
    yearweek(created_at)
    , count(plant_area_id) AS marker_weekly 
FROM
    {db}.assets 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , 
    yearweek(created_at)
ORDER BY
    plant_area_id'''

# measure length week sum through one year based on area ID
q_measure_weekly_change = f'''
SELECT
    plant_area_id
    , yearweek(created_at) year_week
    , count(plant_area_id) AS marker_weekly 
FROM
    {db}.measure_lengths 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , yearweek(created_at) 
ORDER BY
    plant_area_id
'''

# spatial simulation week sum through one year based on area ID
q_simulation_weekly_change = f'''
SELECT
    plant_area_id
    , yearweek(created_at) year_week
    , count(plant_area_id) AS marker_weekly 
FROM
    {db}.plant_area_objects 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , yearweek(created_at) 
ORDER BY
    plant_area_id
'''


# create yearweek with company and area info
def recur_year(company_id, company_name, area_id, area_name):
    q_rec_year = f'''
    WITH RECURSIVE week_dates AS ( 
    SELECT
        {company_id} AS company_id,'{company_name}' AS company_name,{area_id} AS area_id,'{area_name}' AS area_name, DATE ('{minus_one_year}') AS week_date 
    UNION ALL 
    SELECT
         {company_id} AS company_id,'{company_name}' AS company_name,{area_id} AS area_id,'{area_name}' AS area_name,DATE_ADD(week_date, INTERVAL 1 WEEK) 
    FROM
        week_dates 
    WHERE
        DATE_ADD(week_date, INTERVAL 1 WEEK) <= '{running_date}'
) 
SELECT
    company_id,company_name,area_id,area_name,yearweek(week_date) 
FROM
    week_dates;

    '''
    return q_rec_year


# get company and area info
company_areas_results = read_query(conn, q_company_area_id)
# get company and id info
company_areas_ids = read_query(conn, q_company_area_id)

# output company and area info with yearweek with assigned period
with open(weekly_work_files + '\company_areas_date_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name', 'Date'])
    logger.info("company_areas_date_info file is newly created.")

for company_areas_id in company_areas_ids:
    q_com_area_date = recur_year(company_areas_id[0], company_areas_id[1], company_areas_id[2], company_areas_id[3])

    com_area_date_infos = read_query(conn, q_com_area_date)

    with open(weekly_work_files + '\company_areas_date_info' + '.csv', 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for com_area_date_info in com_area_date_infos:
            writer.writerow(
                [com_area_date_info[0], com_area_date_info[1], com_area_date_info[2], com_area_date_info[3],
                 com_area_date_info[4]])

################
# output query result
################

# load
logger.info("Load query contents...")
company_areas_date_info = pd.read_csv(
    weekly_work_files + '/company_areas_date_info.csv')

# users
user_results = read_query(conn, q_user_weekly_change)
# marker
marker_results = read_query(conn, q_marker_weekly_change)
# machine
machine_results = read_query(conn, q_machine_weekly_change)
# measure
measure_results = read_query(conn, q_measure_weekly_change)
# simulation
simulation_results = read_query(conn, q_simulation_weekly_change)
logger.info("Load query contents completed!")

###
# output user result
with open(weekly_work_files + '\\user_weekly_change' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Company ID', 'Date', 'User Weekly Amount'])
    for user_result in user_results:
        writer.writerow(
            [user_result[0], user_result[1], user_result[2]])
logger.info("user_weekly_change csv file is created")

# output marker result
with open(weekly_work_files + '\marker_weekly_change' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Marker Weekly Amount'])
    for marker_result in marker_results:
        writer.writerow(
            [marker_result[0], marker_result[1], int(marker_result[2])])
logger.info("marker_weekly_change csv file is created")

# output machine result
with open(weekly_work_files + '\machine_weekly_change' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Machine Weekly Amount'])
    for machine_result in machine_results:
        writer.writerow(
            [machine_result[0], machine_result[1], int(machine_result[2])])
logger.info("machine_weekly_change csv file is created")

# output measurement result
with open(weekly_work_files + '\measure_weekly_change' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Measurement Weekly Amount'])
    for measure_result in measure_results:
        writer.writerow(
            [measure_result[0], measure_result[1], int(measure_result[2])]
        )
logger.info("measure_weekly_change csv file is created")

# output spatial simulation result
with open(weekly_work_files + '\simulation_weekly_change' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Simulation Weekly Amount'])
    for simulation_result in simulation_results:
        writer.writerow(
            [simulation_result[0], simulation_result[1], int(simulation_result[2])]
        )
logger.info("simulation_weekly_change csv file is created")

# load
logger.info("Loading each csv files...")
user_weekly_change = pd.read_csv(weekly_work_files + '\\user_weekly_change.csv')
marker_weekly_change = pd.read_csv(weekly_work_files + '\marker_weekly_change.csv')
machine_weekly_change = pd.read_csv(weekly_work_files + '\machine_weekly_change.csv')
measure_weekly_change = pd.read_csv(weekly_work_files + '\measure_weekly_change.csv')
simulation_weekly_change = pd.read_csv(weekly_work_files + '\simulation_weekly_change.csv')
logger.info("Each csv files are loaded!")

amm = company_areas_date_info.iloc[:, :5]
amm = pd.merge(amm, user_weekly_change, on=["Company ID", "Date"], how="left")
amm = pd.merge(amm, marker_weekly_change, on=["Plant Area ID", "Date"], how="left")
amm = pd.merge(amm, machine_weekly_change, on=["Plant Area ID", "Date"], how="left")
amm = pd.merge(amm, measure_weekly_change, on=["Plant Area ID", "Date"], how="left")
amm = pd.merge(amm, simulation_weekly_change, on=["Plant Area ID", "Date"], how="left")
# change Date from yearweek format to week's Monday
amm['Date'] = amm['Date'].apply(lambda x: datetime.fromisocalendar(int(str(x)[0:4]), int(str(x)[4:6]), 1))

# output
amm.to_csv(weekly_work_files + '\weekly_transaction_data' + '.csv', encoding="utf-8", index=False)
logger.info("weekly_transaction_data csv file is created!")

# Update BI workbook's weekly transaction sheet.
logger.info("Updating BI file...")
df_weekly_transaction = pd.read_csv(weekly_work_files + '\weekly_transaction_data' + '.csv')
# BI_workbook = xw.Book("brown_reverse_KPI BI.xlsx")
# ws_weekly_transaction = BI_workbook.sheets("weekly_transaction")
# ws_weekly_transaction.cells.clear()
# ws_weekly_transaction.cells(1, 1).options(index=False).value = df_weekly_transaction
#
# BI_workbook.save()
# BI_workbook.close()
logger.info("BI file updating is completed.")
# Close Database connection
# Upload csv file to AWS S3 bucket
logger.info("Upload file to AWS S3 bucket...")
Filename = './weekly_work_files/weekly_transaction_data.csv'
Bucket = kpi_bi_bucket
Key = 'weekly_transaction_data.csv'
client.upload_file(Filename, Bucket, Key)
logger.info('CSV file is uploaded.')


conn.close()
