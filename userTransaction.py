"""
Title: Data Process Script
Description: This script processes raw data and creates csv files.
Author: WangZhibin
Date Created: 2024-05-30
Last Modified: 2024-04-30
Version: 1.0
"""

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
import numpy as np
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

# object export and import file
table_url_csv = ''

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
WITH get_information AS ( 
    SELECT
        company_user_id
        , DATE (created_at) AS date_created_at
        , count(*) AS get_info_count 
    FROM
        {db}.company_logs 
    WHERE
        url = 'api/company/get_information_list' 
        AND DATE (created_at) BETWEEN DATE_SUB('{latest_date}', INTERVAL 1 YEAR) AND '{latest_date}' 
    GROUP BY
        company_user_id
        , DATE (created_at) 
    ORDER BY
        company_user_id
        , DATE (created_at) ASC
) 
SELECT
    cu.id AS company_user_id
    , DATE (cl.created_at)
    , CASE 
        WHEN cl.platform = '1' 
            THEN 'web' 
        WHEN cl.platform = '2' 
            THEN 'ios' 
        ELSE cl.platform 
        END AS access_from
    , gi.get_info_count 
FROM
    {db}.company_logs AS cl 
    LEFT OUTER JOIN {db}.company_users AS cu 
        ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    LEFT OUTER JOIN get_information AS gi 
        ON cu.id = gi.company_user_id 
        AND DATE (cl.created_at) = date_created_at 
WHERE
    url = 'api/company/auth/login' 
    AND DATE (cl.created_at) BETWEEN DATE_SUB('{latest_date}', INTERVAL 1 YEAR) AND '{latest_date}' 
ORDER BY
    cu.id
    , DATE (cl.created_at) ASC
'''

q_distinct_user_activity = f'''
SELECT DISTINCT
    cu.id
    , DATE (cl.created_at) 
FROM
    company_logs AS cl 
    LEFT OUTER JOIN company_users AS cu 
        ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
WHERE
    cl.url = 'api/company/auth/login' 
    AND DATE (cl.created_at) BETWEEN DATE_SUB('{latest_date}', INTERVAL 1 YEAR) AND '{latest_date}' 
ORDER BY
    cu.id
    , DATE (cl.created_at) ASC
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
WITH initial_login_info AS ( 
    SELECT
        cu.id AS company_user_id
        , min(DATE (cl.created_at)) AS initial_login_date 
        , max(DATE (cl.created_at)) AS last_login_date
    FROM
        {db}.company_logs AS cl 
        LEFT OUTER JOIN {db}.company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
    GROUP BY
        cu.id 
    ORDER BY
        cu.id
        , DATE (cl.created_at) ASC
) 
SELECT
    com.id AS company_id
    , com.name AS company_name
    , DATE (cu.created_at) AS registered_date
    , cu.id AS user_id
    , cu.name AS user_name
    , CASE 
        WHEN cu.email LIKE '%@brownreverse%' 
            THEN 1 
        ELSE 0 
        END AS is_brs_user
    , ili.initial_login_date 
    , ili.last_login_date
FROM
    {db}.companies AS com 
    LEFT OUTER JOIN {db}.company_users AS cu 
        ON com.id = cu.company_id 
    LEFT OUTER JOIN initial_login_info AS ili 
        ON cu.id = ili.company_user_id
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
    AND cu.email NOT LIKE '%@brownreverse%'
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
    LEFT OUTER JOIN {db}.pipe_groups AS ps
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
# modified due to platform added
q_marker_daily_info = f'''
SELECT
    m.plant_area_id,
    DATE(m.created_at) AS created_date,
    COUNT(CASE WHEN m.platform IS NULL THEN 1 END) AS marker_daily_amount_unknown,
    COUNT(CASE WHEN m.platform = '1' THEN 1 END) AS marker_daily_amount_web,
    COUNT(CASE WHEN m.platform = '2' THEN 1 END) AS marker_daily_amount_ios,
    COUNT(CASE WHEN m.created_at <> m.updated_at AND m.platform IS NULL AND m.deleted_at IS NULL THEN 1 END) AS marker_daily_updated_amount_unknown,
    COUNT(CASE WHEN m.created_at <> m.updated_at AND m.platform = '1' AND m.deleted_at IS NULL THEN 1 END) AS marker_daily_updated_amount_web,
    COUNT(CASE WHEN m.created_at <> m.updated_at AND m.platform = '2' AND m.deleted_at IS NULL THEN 1 END) AS marker_daily_updated_amount_ios,
    COUNT(CASE WHEN m.deleted_at IS NOT NULL AND m.platform IS NULL THEN 1 END) AS marker_daily_deleted_amount_unknown,
    COUNT(CASE WHEN m.deleted_at IS NOT NULL AND m.platform = '1' THEN 1 END) AS marker_daily_deleted_amount_web,
    COUNT(CASE WHEN m.deleted_at IS NOT NULL AND m.platform = '2' THEN 1 END) AS marker_daily_deleted_amount_ios
FROM
    markers AS m
    LEFT OUTER JOIN company_users AS cu 
        ON m.company_user_id = cu.id 
WHERE
    DATE(m.created_at) BETWEEN DATE_SUB('{latest_date}', INTERVAL 1 YEAR) AND '{latest_date}'
    AND cu.email NOT LIKE '%@brownreverse%' 
GROUP BY
    m.plant_area_id,
    DATE(m.created_at)
ORDER BY
    m.plant_area_id,
    DATE(m.created_at);
'''

# machine daily info
# modified due to platform added
q_machine_daily_info = f'''
SELECT
    a.plant_area_id,
    DATE(a.created_at) AS created_date,
    COUNT(CASE WHEN a.platform IS NULL THEN 1 END) AS machine_daily_amount_unknown,
    COUNT(CASE WHEN a.platform = '1' THEN 1 END) AS machine_daily_amount_web,
    COUNT(CASE WHEN a.platform = '2' THEN 1 END) AS machine_daily_amount_ios,
    COUNT(CASE WHEN a.created_at <> a.updated_at AND a.platform IS NULL THEN 1 END) AS  machine_daily_updated_amount_unknown,
    COUNT(CASE WHEN a.created_at <> a.updated_at AND a.platform='1' THEN 1 END) AS  machine_daily_updated_amount_web,
    COUNT(CASE WHEN a.created_at <> a.updated_at AND a.platform='2' THEN 1 END) AS  machine_daily_updated_amount_ios,
    COUNT(CASE WHEN position_x IS NOT NULL AND a.platform IS NULL THEN 1 END) AS machine_location_daily_amount_unknown,
    COUNT(CASE WHEN position_x IS NOT NULL AND a.platform='1' THEN 1 END) AS machine_location_daily_amount_web,
    COUNT(CASE WHEN position_x IS NOT NULL AND a.platform='2' THEN 1 END) AS machine_location_daily_amount_ios,
    '' AS machine_daily_deleted_amount_unknown,
    '' AS machine_daily_deleted_amount_web,
    '' AS machine_daily_deleted_amount_ios
FROM
    assets AS a
  LEFT OUTER JOIN company_users AS cu 
        ON a.company_user_id = cu.id 
WHERE
    DATE (a.created_at) BETWEEN ('{latest_date}' - INTERVAL 1 YEAR) AND '{latest_date}'
    AND (cu.email NOT LIKE '%@brownreverse%' OR cu.email IS NULL)
GROUP BY
    a.plant_area_id,
    DATE(a.created_at)
ORDER BY 
    a.plant_area_id,
    DATE(a.created_at)
'''

# measure length daily info
# modified due to platform added
q_measurement_length_info = f'''
SELECT
    ml.plant_area_id,
    DATE (ml.created_at),
    COUNT(CASE WHEN ml.platform IS NULL THEN 1 END) AS measurement_lengths_daily_amount_unknown,
    COUNT(CASE WHEN ml.platform =  '1' THEN 1 END) AS measurement_lengths_daily_amount_web,
    COUNT(CASE WHEN ml.platform =  '2' THEN 1 END) AS measurement_lengths_daily_amount_ios,
    COUNT(CASE WHEN ml.created_at <> ml.updated_at AND ml.deleted_at IS NULL AND ml.platform IS NULL THEN 1 END) AS measurement_lengths_daily_updated_amount_unknown,
    COUNT(CASE WHEN ml.created_at <> ml.updated_at AND ml.deleted_at IS NULL AND ml.platform = '1' THEN 1 END) AS measurement_lengths_daily_updated_amount_web,
    COUNT(CASE WHEN ml.created_at <> ml.updated_at AND ml.deleted_at IS NULL AND ml.platform = '2' THEN 1 END) AS measurement_lengths_daily_updated_amount_ios,
    COUNT(CASE WHEN ml.deleted_at IS NOT NULL AND ml.platform IS NULL THEN 1 END) AS measure_lengths_daily_deleted_amount_unknown,
    COUNT(CASE WHEN ml.deleted_at IS NOT NULL AND ml.platform = '1' THEN 1 END) AS measure_lengths_daily_deleted_amount_web,
    COUNT(CASE WHEN ml.deleted_at IS NOT NULL AND ml.platform = '2' THEN 1 END) AS measure_lengths_daily_deleted_amount_ios
FROM
    measure_lengths AS ml
LEFT OUTER JOIN
    company_users AS cu
ON
    ml.company_user_id = cu.id
WHERE
    DATE (ml.created_at) BETWEEN '{latest_date}' - INTERVAL 1 YEAR AND '{latest_date}'
AND
    cu.email NOT LIKE '%@brownreverse%'
GROUP BY
    ml.plant_area_id,DATE (ml.created_at)
ORDER BY
    ml.plant_area_id, DATE(ml.created_at)
'''

# simulation daily info
q_simulation_daily_info = f'''
SELECT
    pao.plant_area_id,
    DATE (pao.created_at),
    COUNT(CASE WHEN pao.platform IS NULL THEN 1 END) AS simulation_daily_amount_unknown,
    COUNT(CASE WHEN pao.platform = '1' THEN 1 END) AS simulation_daily_amount_web,
    COUNT(CASE WHEN pao.platform ='2' THEN 1 END) AS simulation_daily_amount_ios,
    COUNT(CASE WHEN pao.created_at <> pao.updated_at AND pao.deleted_at IS NULL AND pao.platform IS NULL THEN 1 END) AS simulation_daily_updated_amount,
    COUNT(CASE WHEN pao.created_at <> pao.updated_at AND pao.deleted_at IS NULL AND pao.platform ='1' THEN 1 END) AS simulation_daily_updated_amount_web,
    COUNT(CASE WHEN pao.created_at <> pao.updated_at AND pao.deleted_at IS NULL AND pao.platform ='2' THEN 1 END) AS simulation_daily_updated_amount_ios,
    COUNT(CASE WHEN pao.deleted_at IS NOT NULL AND pao.platform IS NULL THEN 1 END) AS simulation_daily_deleted_amount_unknown,
    COUNT(CASE WHEN pao.deleted_at IS NOT NULL AND pao.platform ='1' THEN 1 END) AS simulation_daily_deleted_amount_web,
    COUNT(CASE WHEN pao.deleted_at IS NOT NULL AND pao.platform = '2' IS NULL THEN 1 END) AS simulation_daily_deleted_amount_ios
FROM
    plant_area_objects  AS pao
LEFT OUTER JOIN
    company_users AS cu
ON
    pao.company_user_id = cu.id
WHERE
    DATE (pao.created_at) BETWEEN '{latest_date}' - INTERVAL 1 YEAR AND '{latest_date}'
AND
    cu.email NOT LIKE '%@brownreverse%'
GROUP BY
    pao.plant_area_id,DATE (pao.created_at) 
ORDER BY
    pao.plant_area_id,DATE(pao.created_at)
'''

# pipenavi daily info
q_pipe_daily_info = f'''
SELECT
    pg.plant_area_id,
    DATE (pg.created_at),
    COUNT(CASE WHEN pg.platform IS NULL THEN 1 END) AS pipe_group_daily_amount_unknown,
    COUNT(CASE WHEN pg.platform = '1' THEN 1 END) AS pipe_group_daily_amount_web,
    COUNT(CASE WHEN pg.platform = '2' THEN 1 END) AS pipe_group_daily_amount_ios,
    COUNT(CASE WHEN pg.created_at <> pg.updated_at AND pg.deleted_at IS NULL AND pg.platform IS NULL THEN 1 END) AS pipe_group_daily_updated_amount_unknown,
    COUNT(CASE WHEN pg.created_at <> pg.updated_at AND pg.deleted_at IS NULL AND pg.platform ='1' THEN 1 END) AS pipe_group_daily_updated_amount_web,
    COUNT(CASE WHEN pg.created_at <> pg.updated_at AND pg.deleted_at IS NULL AND pg.platform = '2' THEN 1 END) AS pipe_group_daily_updated_amount_ios,
    COUNT(CASE WHEN deleted_at IS NOT NULL AND pg.platform IS NULL THEN 1 END) AS pipe_group_daily_deleted_amount_unknown,
    COUNT(CASE WHEN deleted_at IS NOT NULL AND pg.platform='1' THEN 1 END) AS pipe_group_daily_deleted_amount_web,
    COUNT(CASE WHEN deleted_at IS NOT NULL AND pg.platform='2' THEN 1 END) AS pipe_group_daily_deleted_amount_ios
FROM
    pipe_groups AS pg
LEFT OUTER JOIN
    company_users AS cu
ON
    pg.company_user_id = cu.id
WHERE
    DATE (pg.created_at) BETWEEN '{latest_date}' - INTERVAL 1 YEAR AND '{latest_date}'
AND
    (cu.email NOT LIKE '%@brownreverse%' OR cu.email IS NULL)
GROUP BY
    pg.plant_area_id,DATE (pg.created_at) 
ORDER BY
    pg.plant_area_id,DATE (pg.created_at) 
'''

# company_area_master
q_company_area_master_infos = f'''
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
'''


# Generate sql for objects' import and export based on area.
def object_import_export_area(table_url_csv):
    import_export_sql = f"""
    SELECT
        CASE 
            WHEN plant_area_id IS NOT NULL 
            THEN plant_area_id 
            ELSE COALESCE( 
                JSON_EXTRACT(request_parameters,'$.plant_area_id')
                , plant_area_id
            ) 
        END AS plant_area_id
        , DATE(created_at)
        , COUNT(*) AS %s
    FROM
        {db}.company_logs
    WHERE
        DATE (created_at) BETWEEN '%s' - INTERVAL 1 YEAR AND '%s' AND url = '%s'
    GROUP BY
        plant_area_id
        , DATE (created_at) 
    """

    sql_queries = {}
    with open(table_url_csv, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                url = row[0]
                count_alias = url.split('/')[-1]
                sql_query = import_export_sql % (count_alias, running_date, running_date, url)
                sql_queries[count_alias] = sql_query

    return sql_queries


# Generate sql for objects' import and export based on company.
def object_import_export_company(table_url_csv):
    import_export_sql = f"""
    WITH company_area_relation AS ( 
    SELECT
        com.id AS company_id
        , pa.id AS plant_area_id 
    FROM
        {db}.companies AS com 
        LEFT OUTER JOIN ( 
            SELECT
                *
                , ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY company_id) AS row_num 
            FROM
                {db}.plants
        ) AS pla 
        ON com.id = pla.company_id 
        LEFT OUTER JOIN ( 
            SELECT
                *
                , ROW_NUMBER() OVER (PARTITION BY plant_id ORDER BY plant_id) AS row_num 
            FROM
                {db}.plant_areas
        ) AS pa 
        ON pla.id = pa.plant_id 
    WHERE
        pla.row_num = 1 
        AND pa.row_num = 1)
    SELECT
        car.plant_area_id
        ,DATE(col.created_at)
        ,count(*) as %s
    FROM
        {db}.company_logs AS col
    LEFT OUTER JOIN
        company_area_relation AS car
    ON
        col.company_id = car.company_id
    WHERE 
         DATE (created_at) BETWEEN '%s' - INTERVAL 1 YEAR AND '%s' AND url = '%s'
    GROUP BY 
        car.plant_area_id,DATE (col.created_at)
    """

    sql_queries = {}
    with open(table_url_csv, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                url = row[0]
                count_alias = url.split('/')[-1]
                sql_query = import_export_sql % (count_alias, running_date, running_date, url)
                sql_queries[count_alias] = sql_query

    return sql_queries


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

# get company area info
q_company_area_master_infos = read_query(conn, q_company_area_master_infos)

# get objects' import and output queries list
object_area_queries = object_import_export_area('object_by_area.csv')

# get objects' import and output queries list
object_company_queries = object_import_export_company('object_by_company.csv')

# output user activity log
with open(user_transaction_files + '\\user_activity_tmp_log' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['User ID', 'Login Date', 'Access From', 'Check info'])
    for user_activity_result in user_activity_results:
        writer.writerow(
            [user_activity_result[0], user_activity_result[1], user_activity_result[2], user_activity_result[3]]
        )
logger.info("user_activity_tmp_log csv file is created")

df_user_activity_log = pd.read_csv(user_transaction_files + '\\user_activity_tmp_log.csv')
# Group by User ID and Login Date, then update 'check info' column
df_user_activity_log['Check info'] = df_user_activity_log.groupby(['User ID', 'Login Date'])['Check info'].transform(
    lambda x: [x.iloc[0] if x.iloc[0] else 0] + [0] * (len(x) - 1))
df_user_activity_log['Check info'].fillna(0, inplace=True)
df_user_activity_log['User ID'].fillna(0, inplace=True)
df_user_activity_log['User ID'] = df_user_activity_log['User ID'].astype('int')
df_user_activity_log['User ID'] = df_user_activity_log['User ID'].astype(str)
df_user_activity_log['User ID'].replace('0', '', inplace=True)

# df_user_activity_log['User ID'] = df_user_activity_log['User ID'].replace(0,'',inplace=True)
df_user_activity_log.to_csv(user_transaction_files + '\\user_activity_log.csv', index=False)

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
    writer.writerow(
        ['Company ID', 'Company Name', 'Registered Date', 'User ID', 'User Name', 'IS BRS User', 'Start Date of Use',
         'Last Login Date', 'Update Date'])
    for user_account_data in user_account_datas:
        writer.writerow(
            [user_account_data[0], user_account_data[1], user_account_data[2], user_account_data[3],
             user_account_data[4], user_account_data[5], user_account_data[6], user_account_data[7], running_date])
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
                     'Object Category', 'Object Registered Date', 'Object Update Date', 'Deleted Date', 'User ID',
                     'User Name'])
    for registered_object_master in registered_object_masters:
        writer.writerow(
            [registered_object_master[0], registered_object_master[1], registered_object_master[2],
             registered_object_master[3], registered_object_master[4],
             registered_object_master[5], registered_object_master[6], registered_object_master[7],
             registered_object_master[8], registered_object_master[9], registered_object_master[10],
             registered_object_master[11]]
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
        ['Plant Area ID', 'Date', 'Marker Daily Registered Unknown', 'Marker Daily Registered Web',
         'Marker Daily Registered iOS', 'Marker Daily Updated Unknown', 'Marker Daily Updated Web',
         'Marker Daily Updated iOS', 'Marker Daily Deleted Unknown', 'Marker Daily Deleted Web',
         'Marker Daily Deleted iOS'])
    for _marker_daily_info in marker_daily_info:
        writer.writerow(
            [_marker_daily_info[0], _marker_daily_info[1], _marker_daily_info[2], _marker_daily_info[3],
             _marker_daily_info[4], _marker_daily_info[5], _marker_daily_info[6],
             _marker_daily_info[7], _marker_daily_info[8], _marker_daily_info[9],
             _marker_daily_info[10]]
        )
logger.info("marker_daily_info csv file was created.")

# output machines daily info
with open(user_transaction_files + '\machine_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Plant Area ID', 'Date', 'Machine Daily Registered Unknown', 'Machine Daily Registered Web',
                     'Machine Daily Registered iOS',
                     'Machine Daily Updated Unknown', 'Machine Daily Updated Web', 'Machine Daily Updated iOS',
                     'Machine Location Daily Registered Unknown', 'Machine Location Daily Registered Web',
                     'Machine Location Daily Registered iOS',
                     'Machine Daily Deleted Unknown', 'Machine Daily Deleted Web', 'Machine Daily Deleted iOS'])
    for _machine_daily_info in machine_daily_info:
        writer.writerow(
            [_machine_daily_info[0], _machine_daily_info[1], _machine_daily_info[2], _machine_daily_info[3],
             _machine_daily_info[4], _machine_daily_info[5], _machine_daily_info[6], _machine_daily_info[7],
             _machine_daily_info[8], _machine_daily_info[9], _machine_daily_info[10], _machine_daily_info[11],
             _machine_daily_info[12], _machine_daily_info[13]]
        )
logger.info("machine_daily_info csv file was created.")

# Output measure length daily info
with open(user_transaction_files + '\measurement_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Measurement Daily Registered Unknown', 'Measurement Daily Registered Web',
         'Measurement Daily Registered iOS', 'Measurement Daily Updated Unknown', 'Measurement Daily Updated Web',
         'Measurement Daily Updated iOS',
         'Measurement Daily Deleted Unknown', 'Measurement Daily Deleted Web', 'Measurement Daily Deleted iOS'])
    for _measurement_daily_info in measurement_daily_info:
        writer.writerow(
            [_measurement_daily_info[0], _measurement_daily_info[1], _measurement_daily_info[2],
             _measurement_daily_info[3], _measurement_daily_info[4], _measurement_daily_info[5],
             _measurement_daily_info[6], _measurement_daily_info[7], _measurement_daily_info[8],
             _measurement_daily_info[9], _measurement_daily_info[10]]
        )
logger.info("measurement_length_daily_info csv file was created.")

# Output simulation daily info
with open(user_transaction_files + '\simulation_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Plant Area ID', 'Date', 'Simulation Daily Registered Unknown', 'Simulation Daily Registered Web',
                     'Simulation Daily Registered iOS',
                     'Simulation Daily Updated Unknown', 'Simulation Daily Updated Web', 'Simulation Daily Updated iOS',
                     'Simulation Dialy Deleted Unknown', 'Simulation Dialy Deleted Web',
                     'Simulation Dialy Deleted iOS'])
    for _simulation_daily_info in simulation_daily_info:
        writer.writerow(
            [_simulation_daily_info[0], _simulation_daily_info[1], _simulation_daily_info[2], _simulation_daily_info[3],
             _simulation_daily_info[4], _simulation_daily_info[5], _simulation_daily_info[6], _simulation_daily_info[7],
             _simulation_daily_info[8], _simulation_daily_info[9], _simulation_daily_info[10]]
        )
logger.info("simulation_daily_info csv file was created.")

# Output pipeNavi daily info
with open(user_transaction_files + '\pipe_daily_info' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'PipeNavi Daily Registered Unknown', 'PipeNavi Daily Registered Web', 'PipeNavi Daily Registered iOS',
         'PipeNavi Daily Updated Unknown', 'PipeNavi Daily Updated Web', 'PipeNavi Daily Updated iOS',
         'PipeNavi Daily Deleted Unknown','PipeNavi Daily Deleted Web','PipeNavi Daily Deleted iOS'])
    for _pipe_daily_info in pipe_daily_info:
        writer.writerow(
            [_pipe_daily_info[0], _pipe_daily_info[1], int(_pipe_daily_info[2]), int(_pipe_daily_info[3]),
             int(_pipe_daily_info[4]),int(_pipe_daily_info[5]),int(_pipe_daily_info[6]),
             int(_pipe_daily_info[7]),int(_pipe_daily_info[8]),int(_pipe_daily_info[9]),
             int(_pipe_daily_info[10])]
        )
logger.info("pipe_segment_daily_info csv file was created.")

# Output company area master
with open(user_transaction_files + '\\company_area_master' + '.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name']
    )
    for q_company_area_master_info in q_company_area_master_infos:
        writer.writerow(
            [q_company_area_master_info[0], q_company_area_master_info[1], q_company_area_master_info[2],
             q_company_area_master_info[3]]
        )
logger.info("company_area_master csv file is created.")

# get object daily info and Output asset export daily info
# _object_daily_info[0].decode() means change byte file into string format
for object, object_sql in object_area_queries.items():
    object_query_daily_info = read_query(conn, object_sql)
    with open(user_transaction_files + '\\' + object + '.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Plant Area ID', 'Date', object])
        for _object_daily_info in object_query_daily_info:
            writer.writerow(
                [_object_daily_info[0].decode(), _object_daily_info[1], _object_daily_info[2]]
            )
    logger.info(object + " csv file was created.")

# _object_daily_info[0].decode() means change byte file into string format
for object, object_sql in object_company_queries.items():
    object_query_daily_info = read_query(conn, object_sql)
    with open(user_transaction_files + '\\' + object + '.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Plant Area ID', 'Date', object])
        for _object_daily_info in object_query_daily_info:
            writer.writerow(
                [_object_daily_info[0], _object_daily_info[1], _object_daily_info[2]]
            )
    logger.info(object + " csv file was created.")

# load csv to dataframe
logger.info("Loading each csv files...")
company_areas_daily_csv_info = pd.read_csv(user_transaction_files + '\company_areas_daily_info.csv')
markers_daily_info = pd.read_csv(user_transaction_files + '\marker_daily_info.csv')
machines_daily_info = pd.read_csv(user_transaction_files + '\machine_daily_info.csv')
measurement_daily_info = pd.read_csv(user_transaction_files + '\measurement_daily_info.csv')
simulation_daily_info = pd.read_csv(user_transaction_files + '\simulation_daily_info.csv')
pipe_daily_info = pd.read_csv(user_transaction_files + '\pipe_daily_info.csv')
# load area object export and import csv to dataframe
export_asset_data_info = pd.read_csv(user_transaction_files + '\export_asset_data.csv')
import_asset_data_info = pd.read_csv(user_transaction_files + '\import_asset_data.csv')
export_tag_info = pd.read_csv(user_transaction_files + '\export_tag.csv')
import_tag_info = pd.read_csv(user_transaction_files + '\import_tag.csv')
export_marker_info = pd.read_csv(user_transaction_files + '\export_marker.csv')
import_marker_info = pd.read_csv(user_transaction_files + '\import_marker.csv')
export_measure_length_info = pd.read_csv(user_transaction_files + '\export_measure_length.csv')
import_measure_length_info = pd.read_csv(user_transaction_files + '\import_measure_length.csv')
export_object_info = pd.read_csv(user_transaction_files + '\export_object.csv')
import_object_info = pd.read_csv(user_transaction_files + '\import_object.csv')
export_pipe_group_xlsx_info = pd.read_csv(user_transaction_files + '\export_pipe_group_xlsx.csv')
import_pipe_group_xlsx_info = pd.read_csv(user_transaction_files + '\import_pipe_group_xlsx.csv')
export_pipe_group_document_xlsx_info = pd.read_csv(user_transaction_files + '\export_pipe_group_document_xlsx.csv')
import_pipe_group_document_xlsx_info = pd.read_csv(user_transaction_files + '\import_pipe_group_document_xlsx.csv')
export_pipe_group_work_xlsx_info = pd.read_csv(user_transaction_files + '\export_pipe_group_work_xlsx.csv')
import_pipe_group_work_xlsx_info = pd.read_csv(user_transaction_files + '\import_pipe_group_work_xlsx.csv')
export_asset_work_xlsx_info = pd.read_csv(user_transaction_files + '\export_asset_work_xlsx.csv')
import_asset_work_xlsx_info = pd.read_csv(user_transaction_files + '\import_asset_work_xlsx.csv')
export_asset_document_xlsx_info = pd.read_csv(user_transaction_files + '\export_asset_document_xlsx.csv')
import_asset_document_xlsx_info = pd.read_csv(user_transaction_files + '\import_asset_document_xlsx.csv')
# load company object export and import csv to dataframe
export_asset_category_info = pd.read_csv(user_transaction_files + '\export_asset_category.csv')
import_asset_category_info = pd.read_csv(user_transaction_files + '\import_asset_category.csv')
export_asset_document_main_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\export_asset_document_main_category_xlsx.csv')
import_asset_document_main_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\import_asset_document_main_category_xlsx.csv')
export_asset_document_sub_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\export_asset_document_sub_category_xlsx.csv')
import_asset_document_sub_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\import_asset_document_sub_category_xlsx.csv')
export_asset_regulation_info = pd.read_csv(user_transaction_files + '\export_asset_regulation.csv')
import_asset_regulation_info = pd.read_csv(user_transaction_files + '\import_asset_regulation.csv')
export_asset_work_large_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\export_asset_work_large_category_xlsx.csv')
import_asset_work_large_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\import_asset_work_large_category_xlsx.csv')
export_asset_work_middle_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\export_asset_work_middle_category_xlsx.csv')
import_asset_work_middle_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\import_asset_work_middle_category_xlsx.csv')
export_asset_work_small_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\export_asset_work_small_category_xlsx.csv')
import_asset_work_small_category_xlsx_info = pd.read_csv(
    user_transaction_files + '\import_asset_work_small_category_xlsx.csv')

# merge object info
cad = company_areas_daily_csv_info.iloc[:, :5]
cad = pd.merge(cad, markers_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, machines_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, measurement_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, simulation_daily_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, pipe_daily_info, on=["Plant Area ID", "Date"], how="left")
# merge area object import and export info based on area
cad = pd.merge(cad, export_asset_data_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_data_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_tag_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_tag_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_marker_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_marker_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_measure_length_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_measure_length_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_object_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_object_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_pipe_group_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_pipe_group_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_pipe_group_document_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_pipe_group_document_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_pipe_group_work_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_pipe_group_work_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_work_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_work_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_document_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_document_xlsx_info, on=["Plant Area ID", "Date"], how="left")
# merge company object import and export info based on area
cad = pd.merge(cad, export_asset_category_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_category_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_document_main_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_document_main_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_document_sub_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_document_sub_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_regulation_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_regulation_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_work_large_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_work_large_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_work_middle_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_work_middle_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, export_asset_work_small_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")
cad = pd.merge(cad, import_asset_work_small_category_xlsx_info, on=["Plant Area ID", "Date"], how="left")

# change columns order
new_column_order = ['Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name',
                    'Date', 'Marker Daily Registered Unknown', 'Marker Daily Registered Web',
                    'Marker Daily Registered iOS', 'Machine Daily Registered Unknown', 'Machine Daily Registered Web',
                    'Machine Daily Registered iOS', 'Measurement Daily Registered Unknown',
                    'Measurement Daily Registered Web', 'Measurement Daily Registered iOS',
                    'Simulation Daily Registered Unknown',  'Simulation Daily Registered Web',  'Simulation Daily Registered iOS',
                    'PipeNavi Daily Registered Unknown','PipeNavi Daily Registered Web','PipeNavi Daily Registered iOS',
                    'Machine Location Daily Registered Unknown', 'Machine Location Daily Registered Web',
                    'Machine Location Daily Registered iOS',
                    'Marker Daily Updated Unknown', 'Marker Daily Updated Web', 'Marker Daily Updated iOS',
                    'Machine Daily Updated Unknown', 'Machine Daily Updated Web', 'Machine Daily Updated iOS',
                    'Measurement Daily Updated Unknown', 'Measurement Daily Updated Web',
                    'Measurement Daily Updated iOS',
                    'Simulation Daily Updated Unknown','Simulation Daily Updated Web','Simulation Daily Updated iOS',
                    'PipeNavi Daily Updated Unknown',   'PipeNavi Daily Updated Web',   'PipeNavi Daily Updated iOS',
                    'Marker Daily Deleted Unknown', 'Marker Daily Deleted Web',
                    'Marker Daily Deleted iOS', 'Machine Daily Deleted Unknown', 'Machine Daily Deleted Web',
                    'Machine Daily Deleted iOS',
                    'Measurement Daily Deleted Unknown', 'Measurement Daily Deleted Web',
                    'Measurement Daily Deleted iOS',
                    'Simulation Dialy Deleted Unknown',  'Simulation Dialy Deleted Web',  'Simulation Dialy Deleted iOS',
                    'PipeNavi Daily Deleted unknown', 'PipeNavi Daily Deleted Web', 'PipeNavi Daily Deleted iOS',
                    'export_marker', 'import_marker', 'export_measure_length', 'import_measure_length', 'export_object',
                    'import_object', 'export_asset_data', 'import_asset_data', 'export_pipe_group_xlsx',
                    'import_pipe_group_xlsx',
                    'export_asset_regulation', 'import_asset_regulation', 'export_asset_category',
                    'import_asset_category', 'export_asset_document_xlsx',
                    'import_asset_document_xlsx', 'export_pipe_group_document_xlsx', 'import_pipe_group_document_xlsx',
                    'export_asset_document_main_category_xlsx', 'import_asset_document_main_category_xlsx',
                    'export_asset_document_sub_category_xlsx', 'import_asset_document_sub_category_xlsx', 'export_tag',
                    'import_tag', 'export_asset_work_xlsx', 'import_asset_work_xlsx', 'export_pipe_group_work_xlsx',
                    'import_pipe_group_work_xlsx', 'export_asset_work_large_category_xlsx',
                    'import_asset_work_large_category_xlsx', 'export_asset_work_middle_category_xlsx',
                    'import_asset_work_middle_category_xlsx',
                    'export_asset_work_small_category_xlsx', 'import_asset_work_small_category_xlsx'
                    ]

# Loop through the list starting from the 6th element to the last and fillna(0)
for value in new_column_order[5:]:
    cad[value] = cad[value].fillna(0)

cad = cad.reindex(columns=new_column_order)

# Rename object export and import columns' name
cad.rename(
    columns={"export_marker": "Marker Daily Exported", "import_marker": "Marker Daily Imported",
             "export_measure_length": "Measurement Daily Exported",
             "import_measure_length": "Measurement Daily Imported", "export_object": "Simulation Daily Exported",
             "import_object": "Simulation Daily Imported",
             "export_asset_data": "Machine Daily Exported", "import_asset_data": "Machine Daily Imported",
             "export_pipe_group_xlsx": "Pipe Daily Exported", "import_pipe_group_xlsx": "Pipe Daily Imported",
             "export_asset_regulation": "Regulations Daily Exported",
             "import_asset_regulation": "Regulations Daily Imported",
             "export_asset_category": "Machine Category Daily Exported",
             "import_asset_category": "Machine Category Daily Imported",
             "export_asset_document_xlsx": "Books Daily Exported", "import_asset_document_xlsx": "Books Daily Imported",
             "export_pipe_group_document_xlsx": "Books_pipe Daily Exported",
             "import_pipe_group_document_xlsx": "Books_pipe Daily Imported",
             "export_asset_document_main_category_xlsx": "Books_main_category Daily Exported",
             "import_asset_document_main_category_xlsx": "Books_main_category Daily Imported",
             "export_asset_document_sub_category_xlsx": "Books_sub_category Daily Exported",
             "import_asset_document_sub_category_xlsx": "Books_sub_category Daily Imported",
             "export_tag": "Tag Daily Exported", "import_tag": "Tag Dialy Imported",
             "export_asset_work_xlsx": "History Planning Daily Exported",
             "import_asset_work_xlsx": "History Planning Daily Imported",
             "export_pipe_group_work_xlsx": "History Planning_pipe Daily Exported",
             "import_pipe_group_work_xlsx": "History Planning_pipe Daily Imported",
             "export_asset_work_large_category_xlsx": "History Planning_large_category Daily Exported",
             "import_asset_work_large_category_xlsx": "History Planning_large_category Daily Imported",
             "export_asset_work_middle_category_xlsx": "History Planning_Medium_category Daily Exported",
             "import_asset_work_middle_category_xlsx": "History Planning_Medium_category Daily Imported",
             "export_asset_work_small_category_xlsx": "History Planning_Small_category Daily Exported",
             "import_asset_work_small_category_xlsx": "History Planning_Small_category Daily Imported"
             },
    inplace=True
)

# output
cad.to_csv(user_transaction_files + '\daily_object_transaction_data' + '.csv', encoding="utf-8", index=False)
logger.info("daily_object_transaction_data csv file is created.")
