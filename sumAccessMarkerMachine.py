# MySQLdbのインポート
import csv
import datetime
import time

import mysql.connector
from mysql.connector import Error

# 日付情報
running_date = datetime.date.today()
temp_running_date = '2023/07/04'

# 　日付時刻情報
timestr = time.strftime("%Y%m%d%H%M%S")

# データベースのスキーマ
schema_dev = "kpi_work"
schema_prod = "kpi_work_prod"

# DBアカウント情報
pd = 'KpI_Viewer3d'
# db値、スキーマのと一致
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
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

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
        print(f"Error: '{err}'")


################
# クエリ
################

# 日/週/月ごとに1回以上ログインしたユーザ数
q_access_amount = f'''
WITH day_access AS ( 
    SELECT
        company_id
        , company_user_id
        , DATE (created_at) AS access_date 
    FROM
        {db}.company_logs 
    WHERE
        url = 'api/company/auth/me' 
        AND DATE (created_at) = '{temp_running_date}'
) 

, week_access AS ( 
    SELECT
        company_id
        , company_user_id
        , DATE (created_at) AS access_date 
    FROM
        {db}.company_logs 
    WHERE
        url = 'api/company/auth/me' 
        AND DATE (created_at) BETWEEN '{temp_running_date}' - INTERVAL 1 week AND '{temp_running_date}'
) 

, month_access AS ( 
    SELECT
        company_id
        , company_user_id
        , DATE (created_at) AS access_date 
    FROM
        {db}.company_logs 
    WHERE
        url = 'api/company/auth/me' 
        AND DATE (created_at) BETWEEN '{temp_running_date}' - INTERVAL 1 MONTH AND '{temp_running_date}'
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
GROUP BY
    com.id
    , com.name
    , pla.id
    , pla.name
    , pa.id
    , pa.name

'''

conn = create_db_connection('localhost', 'root', pd, db)
results = read_query(conn, q_access_amount)

with open(f"access_marker_machine_amounts.csv", 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Update Date', 'Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name', 'Day Access Amount',
         'Day Access Ratio (%)', 'Week Access Amount', 'Week Access Ratio (%)', 'Month Access Amount', 'Month Access Ratio (%)',
         'Marker Total Registered', 'Machine Total Registered'])
    for result in results:
        writer.writerow(
            [running_date, result[0], result[1], result[2], result[3], result[4], result[5], result[6],
             result[7], result[8], result[9], result[10], result[11]])

# 接続を閉じる
conn.close()
