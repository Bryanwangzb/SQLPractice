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

q_user_activity = f'''
SELECT 
    company_user_id
    , DATE(created_at)
FROM
    {db}.company_logs 
WHERE
    url = 'api/company/auth/me' 
    AND DATE (created_at) BETWEEN DATE_SUB('{latest_date}', INTERVAL 1 YEAR) AND '{latest_date}'
ORDER BY company_user_id,DATE(created_at) asc
'''

q_distinct_user_activity = f'''
SELECT DISTINCT
    company_user_id
    , DATE(created_at)
FROM
    {db}.company_logs 
WHERE
    url = 'api/company/auth/me' 
    AND DATE (created_at) BETWEEN DATE_SUB('{latest_date}', INTERVAL 1 YEAR) AND '{latest_date}'
ORDER BY company_user_id,DATE(created_at) asc
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

# user account info
q_user_account = f'''
SELECT
    com.id AS company_id
    , com.name AS company_name
    , DATE (cu.created_at) AS registered_date
    , cu.id AS user_id
    , cu.name AS user_name 
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.company_users AS cu 
        ON com.id = cu.company_id
'''

q_user_account_total_count = f'''
WITH COMPANY_ONE_YEAR AS ( 
    WITH RECURSIVE DateRange AS ( 
        SELECT
            CURDATE() AS DATE
            , 1 AS n 
        UNION ALL 
        SELECT
            DATE_SUB(DATE, INTERVAL 1 DAY)
            , n + 1 
        FROM
            DateRange 
        WHERE
            n < 365
    ) 
    SELECT
        cu.id AS company_id
        , cu.name AS company_name
        , dr.DATE AS DATE 
    FROM
        {db}.companies cu 
        CROSS JOIN DateRange dr 
    ORDER BY
        cu.id
        , dr.DATE ASC
) 
SELECT
    t.company_id
    , t.company_name
    , t.DATE
    , count(cu.id) as account_sum
FROM
    COMPANY_ONE_YEAR AS t 
    LEFT OUTER JOIN company_users AS cu 
        ON t.company_id = cu.company_id 
WHERE
    DATE (created_at) < t.DATE 
GROUP BY
    t.company_id
    , t.company_name
    , DATE 
ORDER BY
    t.company_id
'''

q_registered_object_master = f'''
WITH plant_master AS (
SELECT
    com.id AS company_id
    , com.name AS company_name
    , pa.id AS plant_area_id
    , pa.name AS plant_area_name
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN {db}.plant_areas AS pa 
        ON pla.id = pa.plant_id 
WHERE
    pa.id IS NOT NULL 
UNION 
SELECT DISTINCT
    com.id AS company_id
    , com.name AS company_name
    , m.plant_area_id AS plant_area_id
    ,'' AS plant_area_name
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN {db}.plant_areas AS pa 
        ON pla.id = pa.plant_id 
    LEFT OUTER JOIN {db}.company_users AS cu 
        ON com.id = cu.company_id 
    LEFT OUTER JOIN {db}.markers AS m
        ON cu.id=m.company_user_id
    LEFT OUTER JOIN {db}.measure_lengths as ml
        ON cu.id=ml.company_user_id
    LEFT OUTER JOIN {db}.plant_area_objects as pao
        ON cu.id=pao.company_user_id
    WHERE pa.id IS NULL AND m.plant_area_id IS NOT NULL
)
SELECT
    pm.company_id,
    pm.company_name,
    pm.plant_area_id,
    pm.plant_area_name,
    ast.id AS object_id,
    'machine' AS object_category,
    ast.name AS object_name,
    DATE(ast.created_at) AS object_registered_date,
    CASE
        WHEN ast.created_at <> ast.updated_at THEN DATE(ast.updated_at)
        ELSE ''
    END AS updated_date,
    '' AS deleted_date,
    '' AS user_id,
    '' AS user_name
FROM
    plant_master AS pm
    LEFT OUTER JOIN {db}.assets AS ast
        ON pm.plant_area_id = ast.plant_area_id
WHERE
    ast.id IS NOT NULL
UNION ALL
SELECT
    pm.company_id,
    pm.company_name,
    pm.plant_area_id,
    pm.plant_area_name,
    mk.id AS object_id,
    'marker' AS object_category,
    mk.name AS object_name,
    DATE(mk.created_at) AS object_registered_date,
    CASE
        WHEN mk.created_at <> mk.updated_at AND mk.deleted_at IS NULL THEN DATE(mk.updated_at)
        WHEN mk.created_at <> mk.updated_at AND mk.deleted_at IS NOT NULL THEN ''
        ELSE ''
    END AS updated_date,
    CASE
        WHEN mk.deleted_at IS NOT NULL THEN DATE(mk.deleted_at)
        ELSE ''
    END AS deleted_date,
    mk.company_user_id as user_id,
    cu.name as user_name
FROM
    plant_master AS pm
    LEFT OUTER JOIN {db}.markers AS mk
        ON pm.plant_area_id = mk.plant_area_id
    LEFT OUTER JOIN {db}.company_users as cu
        ON mk.company_user_id = cu.id
WHERE
    mk.id IS NOT NULL
UNION ALL
SELECT
    pm.company_id,
    pm.company_name,
    pm.plant_area_id,
    pm.plant_area_name,
    ps.id AS object_id,
    'pipe navi' AS object_category,
    '' AS object_name,
    DATE(ps.created_at) AS object_registered_date,
    CASE
        WHEN ps.created_at <> ps.updated_at AND ps.deleted_at IS NULL THEN DATE(ps.updated_at)
        WHEN ps.created_at <> ps.updated_at AND ps.deleted_at IS NOT NULL THEN ''
        ELSE ''
    END AS updated_date,
    CASE
        WHEN ps.deleted_at IS NOT NULL THEN DATE(ps.deleted_at)
        ELSE ''
    END AS deleted_date,
    '' AS user_id,
    '' AS user_name
FROM
    plant_master AS pm
    LEFT OUTER JOIN {db}.pipe_segments AS ps
        ON pm.plant_area_id = ps.plant_area_id
WHERE
    ps.id IS NOT NULL
UNION ALL
SELECT
    pm.company_id,
    pm.company_name,
    pm.plant_area_id,
    pm.plant_area_name,
    ml.id AS object_id,
    'measurement' AS object_category,
    ml.name AS object_name,
    DATE(ml.created_at) AS object_registered_date,
    CASE
        WHEN ml.created_at <> ml.updated_at AND ml.deleted_at IS NULL THEN DATE(ml.updated_at)
        WHEN ml.created_at <> ml.updated_at AND ml.deleted_at IS NOT NULL THEN ''
        ELSE ''
    END AS updated_date,
    CASE
        WHEN ml.deleted_at IS NOT NULL THEN DATE(ml.deleted_at)
        ELSE ''
    END AS deleted_date,
    ml.company_user_id AS user_id,
    cu.name AS user_name
FROM
    plant_master AS pm
    LEFT OUTER JOIN {db}.measure_lengths AS ml
        ON pm.plant_area_id = ml.plant_area_id
    LEFT OUTER JOIN company_users as cu
        ON ml.company_user_id = cu.id      
WHERE
    ml.id IS NOT NULL
UNION ALL
SELECT
    pm.company_id,
    pm.company_name,
    pm.plant_area_id,
    pm.plant_area_name,
    pao.id AS object_id,
    'simulation' AS object_category,
    pao.name AS object_name,
    DATE(pao.created_at) AS object_registered_date,
    CASE
        WHEN pao.created_at <> pao.updated_at AND pao.deleted_at IS NULL THEN DATE(pao.updated_at)
        WHEN pao.created_at <> pao.updated_at AND pao.deleted_at IS NOT NULL THEN ''
        ELSE ''
    END AS updated_date,
    CASE
        WHEN pao.deleted_at IS NOT NULL THEN DATE(pao.deleted_at)
        ELSE ''
    END AS deleted_date,
    pao.company_user_id as user_id,
    cu.name as user_name
FROM
    plant_master AS pm
    LEFT OUTER JOIN {db}.plant_area_objects AS pao
        ON pm.plant_area_id = pao.plant_area_id
    LEFT OUTER JOIN company_users as cu
        ON pao.company_user_id = cu.id 
WHERE
    pao.id IS NOT NULL;
'''

q_company_areas_daily_info = f'''
WITH RECURSIVE DateRange AS ( 
        SELECT
            CURDATE() AS daily_date
            , 1 AS n 
        UNION ALL 
        SELECT
            DATE_SUB(daily_date, INTERVAL 1 DAY)
            , n + 1 
        FROM
            DateRange 
        WHERE
            n < 365
    ) 
    SELECT
        cu.id AS company_id
        , cu.name AS company_name
        , pa.id as plant_id
        , pa.name as plant_name
        , dr.daily_date AS daily_date 
    FROM
        companies as cu 
    LEFT OUTER JOIN 
        plants as pl
    on
        cu.id=pl.company_id
    LEFT OUTER JOIN
        plant_areas as pa
    on
        pl.id=pa.plant_id
        CROSS JOIN DateRange dr 
    ORDER BY
        cu.id
        ,pa.id
        , dr.daily_date ASC
'''
# markers daily info
q_marker_daily_info = f'''
SELECT
    plant_area_id
    , date (created_at)
    , COUNT(*) AS marker_daily_amount
    , COUNT( 
        CASE 
            WHEN created_at <> updated_at 
            and deleted_at is null 
                THEN 1 
            END
    ) as marker_daily_updated_amount
    , COUNT(CASE WHEN deleted_at IS NOT NULL THEN 1 END) as marker_daily_deleted_amount 
FROM
    markers 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , date (created_at)
'''

# machine daily info
q_machine_daily_info = f'''
SELECT
    plant_area_id
    , date(created_at)
    , count(*) AS machine_daily_amount 
    ,COUNT(CASE WHEN created_at <> updated_at THEN 1 END) as  machine_daily_updated_amount
    ,COUNT(CASE WHEN position_x is not null then 1 END) as machine_location_daily_amount
    ,'' as machine_daily_deleted_amount
FROM
    assets 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id,date(created_at)
'''

# measure length daily info
q_measurement_length_info = f'''
SELECT
    plant_area_id
    , date (created_at)
    , count(*) AS measurement_lengths_daily_amount
    , COUNT( 
        CASE 
            WHEN created_at <> updated_at 
            and deleted_at is null 
                THEN 1 
            END
    ) as measurement_lengths_daily_updated_amount
    , COUNT(CASE WHEN deleted_at IS NOT NULL THEN 1 END) as measure_lengths_daily_deleted_amount 
FROM
    measure_lengths 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , date (created_at) 
'''

# simulation daily info
q_simulation_daily_info = f'''
SELECT
    plant_area_id
    , date (created_at)
    , count(*) AS simulation_daily_amount
    , COUNT( 
        CASE 
            WHEN created_at <> updated_at 
            and deleted_at is null 
                THEN 1 
            END
    ) as simulation_daily_updated_amount
    , COUNT(CASE WHEN deleted_at IS NOT NULL THEN 1 END) as simulation_daily_deleted_amount 
FROM
    plant_area_objects 
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , date (created_at) 
ORDER BY
    plant_area_id
'''

# pipenavi daily info
q_pipe_daily_info = f'''
SELECT
    plant_area_id
    , date (created_at)
    , count(*) AS pipe_segment_daily_amount
    , COUNT( 
        CASE 
            WHEN created_at <> updated_at 
            and deleted_at is null 
                THEN 1 
            END
    ) as pipe_segment_daily_updated_amount
    , COUNT(CASE WHEN deleted_at IS NOT NULL THEN 1 END) as pipe_segment_daily_deleted_amount 
FROM
    pipe_segments
WHERE
    DATE (created_at) BETWEEN '{running_date}' - INTERVAL 1 YEAR AND '{running_date}'
GROUP BY
    plant_area_id
    , date (created_at) 

'''

################
# 実行結果
################
user_activity_results = read_query(conn, q_user_activity)
user_distinct_activity_results = read_query(conn, q_distinct_user_activity)

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

# get user account data info
user_account_datas = read_query(conn, q_user_account)

# get user account total count
user_account_total_counts = read_query(conn, q_user_account_total_count)

# get registered object master
registered_object_masters = read_query(conn, q_registered_object_master)

# get company areas daily info
company_areas_daily_info = read_query(conn, q_company_areas_daily_info)

# get marker daily info
marker_daily_info = read_query(conn, q_marker_daily_info)

# get machine daily info
machine_daily_info = read_query(conn, q_machine_daily_info)

# get measure length daily info
measurement_daily_info = read_query(conn, q_measurement_length_info)

# get simulation daily info
simulation_daily_info = read_query(conn, q_simulation_daily_info)

# get pipeNavi daily info
pipe_daily_info = read_query(conn, q_pipe_daily_info)

# output user activity log
with open(user_transaction_files + '\\user_activity_log' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['User ID', 'Login Date'])
    for user_activity_result in user_activity_results:
        writer.writerow(
            [user_activity_result[0], user_activity_result[1]]
        )
logger.info("user_activity_log csv file is created")

# output user distinct activity log
with open(user_transaction_files + '\\user_distinct_activity_log' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['User ID', 'Login Date'])
    for user_distinct_activity_result in user_distinct_activity_results:
        writer.writerow(
            [user_distinct_activity_result[0], user_distinct_activity_result[1]]
        )
logger.info("user_distinct_activity_log csv file is created")

# output user account data
with open(user_transaction_files + '\\user_account_data' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Company ID', 'Company Name', 'Registered Date', 'User ID', 'User Name', 'Update Date'])
    for user_account_data in user_account_datas:
        writer.writerow(
            [user_account_data[0], user_account_data[1], user_account_data[2], user_account_data[3],
             user_account_data[4], running_date])
logger.info("user_account_data csv file is created")

# output user account total count
with open(user_transaction_files + '\\user_account_total_count' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow((['Company ID', 'Company Name', 'Date', 'Account Total Count']))
    for user_account_total_count in user_account_total_counts:
        writer.writerow(
            [user_account_total_count[0], user_account_total_count[1], user_account_total_count[2],
             user_account_total_count[3]]
        )
logger.info("user_account_total_count csv file is created")

# output registered object master
with open(user_transaction_files + '\\registered_object_master' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name', 'Object ID', 'Object Name',
                     'Object Category', 'Object Registered Date', 'Object Update Date', 'Deleted Date'])
    for registered_object_master in registered_object_masters:
        writer.writerow(
            [registered_object_master[0], registered_object_master[1], registered_object_master[2],
             registered_object_master[3], registered_object_master[4],
             registered_object_master[5], registered_object_master[6], registered_object_master[7],
             registered_object_master[8], registered_object_master[9]]
        )
logger.info("registered_object_master csv file is created")

# output company areas info
with open(user_transaction_files + '\company_areas_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name', 'Date'])
    for _company_areas_daily_info in company_areas_daily_info:
        writer.writerow(
            [_company_areas_daily_info[0], _company_areas_daily_info[1], _company_areas_daily_info[2],
             _company_areas_daily_info[3], _company_areas_daily_info[4]]
        )
logger.info("company_areas_daily_info csv file was created.")

# output markers daily info
with open(user_transaction_files + '\marker_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Marker Daily Registered', 'Marker Daily Updated', 'Marker Daily Deleted'])
    for _marker_daily_info in marker_daily_info:
        writer.writerow(
            [_marker_daily_info[0], _marker_daily_info[1], _marker_daily_info[2], _marker_daily_info[3],
             _marker_daily_info[4]]
        )
logger.info("marker_daily_info csv file was created.")

# output machines daily info
with open(user_transaction_files + '\machine_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Plant Area ID', 'Date', 'Machine Daily Registered', 'Machine Daily Updated',
                     'Machine Location Daily Registered', 'Machine Daily Deleted'])
    for _machine_daily_info in machine_daily_info:
        writer.writerow(
            [_machine_daily_info[0], _machine_daily_info[1], _machine_daily_info[2], _machine_daily_info[3],
             _machine_daily_info[4], _machine_daily_info[5]]
        )
logger.info("machine_daily_info csv file was created.")

# Output measure length daily info
with open(user_transaction_files + '\measurement_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Plant Area ID', 'Date', 'Measurement Daily Registered', 'Measurement Daily Updated',
                     'Measurement Daily Deleted'])
    for _measurement_daily_info in measurement_daily_info:
        writer.writerow(
            [_measurement_daily_info[0], _measurement_daily_info[1], _measurement_daily_info[2],
             _measurement_daily_info[3], _measurement_daily_info[4]]
        )
logger.info("measurement_length_daily_info csv file was created.")

# Output simulation daily info
with open(user_transaction_files + '\simulation_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Plant Area ID', 'Date', 'Simulation Daily Registered', 'Simulation Daily Updated',
                     'Simulation Dialy Deleted'])
    for _simulation_daily_info in simulation_daily_info:
        writer.writerow(
            [_simulation_daily_info[0], _simulation_daily_info[1], _simulation_daily_info[2], _simulation_daily_info[3],
             _simulation_daily_info[4]]
        )
logger.info("simulation_daily_info csv file was created.")

# Output pipeNavi daily info
with open(user_transaction_files + '\pipe_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'PipeNavi Daily Registered', 'PipeNavi Daily Updated', 'PipeNavi Daily Deleted'])
    for _pipe_daily_info in pipe_daily_info:
        writer.writerow(
            [_pipe_daily_info[0], _pipe_daily_info[1], int(_pipe_daily_info[2]), int(_pipe_daily_info[3]), int(_pipe_daily_info[4])]
        )
logger.info("pipe_segment_daily_info csv file was created.")

# load
logger.info("Loading each csv files...")
company_areas_daily_csv_info = pd.read_csv(user_transaction_files + '\company_areas_daily_info.csv')
markers_daily_info = pd.read_csv(user_transaction_files + '\marker_daily_info.csv')
machines_daily_info = pd.read_csv(user_transaction_files + '\machine_daily_info.csv')
measurement_daily_info = pd.read_csv(user_transaction_files + '\measurement_daily_info.csv')
simulation_daily_info = pd.read_csv(user_transaction_files + '\simulation_daily_info.csv')
pipe_daily_info = pd.read_csv(user_transaction_files + '\pipe_daily_info.csv')

# merge
cad = company_areas_daily_csv_info.iloc[:, :5]
cad = pd.merge(cad, markers_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, machines_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, measurement_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, simulation_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, pipe_daily_info, on=["Plant Area ID", "Date"], how="left")

# change columns order
new_column_order = ['Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name',
                    'Date', 'Marker Daily Registered', 'Machine Daily Registered', 'Measurement Daily Registered',
                    'Simulation Daily Registered', 'PipeNavi Daily Registered', 'Machine Location Daily Registered',
                    'Marker Daily Updated', 'Machine Daily Updated', 'Measurement Daily Updated',
                    'Simulation Daily Updated',
                    'PipeNavi Daily Updated', 'Marker Daily Deleted', 'Machine Daily Deleted',
                    'Measurement Daily Deleted', 'Simulation Dialy Deleted', 'PipeNavi Daily Deleted']

cad = cad.reindex(columns=new_column_order)
# output
cad.to_csv(user_transaction_files + '\daily_object_transaction_data' + '.csv', encoding="utf-8", index = False)
logger.info("daily_object_transaction_data csv file is created.")