import csv
import datetime
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta

import mysql.connector
import pandas as pd
from mysql.connector import Error

# Database account information
pwd = 'KpI_Viewer3d'
db = 'kpi_work'

# Date information
# Temporary using temp_running_date for developing, but need change to running_date in the productive environment
#running_date = datetime.strptime(str(datetime.now()),'%Y-%m-%d').date()
#print(running_date)
temp_running_date = '2023/07/04'
# following for developing
minus_one_year = datetime.strptime(temp_running_date,'%Y/%m/%d').date() - relativedelta(years=1)
# following for productive
# minus_one_year = running_date - relativedelta(years=1)
print(minus_one_year)

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
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

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
        print(f"Error: '{err}'")


################
# Query
################

# # Comapny and area info
# q_company_area = '''
# SELECT
#     com.id AS company_id
#     , com.name as company_name
#     , pa.id AS area_id
#     , pa.name as area_name
# FROM
#     kpi_work.companies AS com
#     LEFT OUTER JOIN kpi_work.plants AS pla
#         ON com.id = pla.company_id
#     LEFT OUTER JOIN kpi_work.plant_areas AS pa
#         ON pla.id = pa.plant_id
#
# '''

# Comapny and area info
q_company_area_id = '''
SELECT
    com.id AS company_id
    , com.name as company_name
    , pa.id AS area_id 
    , pa.name as area_name
FROM
    kpi_work.companies AS com 
    LEFT OUTER JOIN kpi_work.plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN kpi_work.plant_areas AS pa 
        ON pla.id = pa.plant_id 
WHERE
    pla.id IS NOT NULL 
    AND pa.id IS NOT NULL

'''

# User login times through one year depends on Company ID
q_user_weekly_change = f'''
WITH year_user_num AS ( 
    WITH year_change_week AS ( 
        WITH year_access AS ( 
            SELECT
                company_id
                , company_user_id
                , DATE (created_at) AS access_date 
            FROM
                kpi_work.company_logs 
            WHERE
                url = 'api/company/auth/me' 
                AND DATE (created_at) BETWEEN '{temp_running_date}' - INTERVAL 1 YEAR AND '{temp_running_date}'
        ) 
        SELECT
            company_id
            , company_user_id
            , yearweek(access_date) AS year_week 
        FROM
            year_access 
        GROUP BY
            company_id
            , company_user_id
            , yearweek(access_date)
    ) 
    SELECT
        company_id
        , year_week
        , company_user_id 
    FROM
        year_change_week 
    GROUP BY
        company_id
        , company_user_id
        , year_week
) 
SELECT
    company_id
    , 
    year_week 
    , count(company_user_id) AS weekly_amount 
FROM
    year_user_num 
GROUP BY
    company_id
    , year_week 
ORDER BY
    company_id

'''

# 　マーカーの週ごとの合計値を直近1年分
q_marker_weekly_change = f'''
SELECT
    plant_area_id
    , 
    yearweek(created_at)year_week
    , count(plant_area_id) AS marker_weekly 
FROM
    kpi_work.markers 
WHERE
    DATE (created_at) BETWEEN '{temp_running_date}' - INTERVAL 1 YEAR AND '{temp_running_date}'
GROUP BY
    plant_area_id
    , 
    yearweek(created_at)
  
ORDER BY
    plant_area_id

'''

# 　機番の週ごとの合計値を直近1年分
q_machine_weekly_change = f'''
SELECT
    plant_area_id
    , 
    yearweek(created_at)
    , count(plant_area_id) AS marker_weekly 
FROM
    kpi_work.assets 
WHERE
    DATE (created_at) BETWEEN '{temp_running_date}' - INTERVAL 1 YEAR AND '{temp_running_date}'
GROUP BY
    plant_area_id
    , 
    yearweek(created_at)
ORDER BY
    plant_area_id'''



def recur_year(company_id, company_name, area_id, area_name):
    q_rec_year = f'''
    WITH RECURSIVE week_dates AS ( 
    SELECT
        {company_id} AS company_id,'{company_name}' AS company_name,{area_id} AS area_id,'{area_name}' AS area_name, DATE ('2022-07-04') AS week_date 
    UNION ALL 
    SELECT
         {company_id} AS company_id,'{company_name}' AS company_name,{area_id} AS area_id,'{area_name}' AS area_name,DATE_ADD(week_date, INTERVAL 1 WEEK) 
    FROM
        week_dates 
    WHERE
        DATE_ADD(week_date, INTERVAL 1 WEEK) <= '2023-07-04'
) 
SELECT
    company_id,company_name,area_id,area_name,yearweek(week_date) 
FROM
    week_dates;

    '''
    return q_rec_year

conn = create_db_connection('localhost', 'root', pwd, db)
# companyとarea
company_areas_results = read_query(conn, q_company_area_id)

# companyとareaのID情報
company_areas_ids = read_query(conn, q_company_area_id)

with open(f"company_areas_date_info.csv", 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Company ID', 'Company Name', 'Plant Area ID', 'Plant Area Name', 'Date'])

for company_areas_id in company_areas_ids:
    q_com_area_date = recur_year(company_areas_id[0], company_areas_id[1], company_areas_id[2], company_areas_id[3])

    com_area_date_infos = read_query(conn, q_com_area_date)

    with open(f"company_areas_date_info.csv", 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for com_area_date_info in com_area_date_infos:
            writer.writerow(
                [com_area_date_info[0], com_area_date_info[1], com_area_date_info[2], com_area_date_info[3],
                 com_area_date_info[4]])

################
# クエリ結果を出力
################
# companyとareaの情報をまとめてCSVに出力
with open(f"company_areas_info.csv", 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Company ID', 'Company Name', 'Plant Area ID', 'Area Name'])
    for company_areas_result in company_areas_results:
        writer.writerow(
            [company_areas_result[0], company_areas_result[1], company_areas_result[2],
             company_areas_result[3]])

# load
company_areas_date_info = pd.read_csv(
    "C:/Users/WorkAccount/PycharmProjects/pythonProject/venv/company_areas_date_info.csv")


# ユーザ
user_results = read_query(conn, q_user_weekly_change)
# 　マーカー
marker_results = read_query(conn, q_marker_weekly_change)
# 機番
machine_results = read_query(conn, q_machine_weekly_change)

# ユーザ結果をCSVに出力
with open(f"user_weekly_change.csv", 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Company ID', 'Date', 'User Weekly Amount'])
    for user_result in user_results:
        writer.writerow(
            [user_result[0], user_result[1], user_result[2]])

# マーカー結果をCSVに出力
with open(f"marker_weekly_change.csv", 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Marker Weekly Amount'])
    for marker_result in marker_results:
        writer.writerow(
            [marker_result[0], marker_result[1],int(marker_result[2])])

# 機番結果をCSVに出力
with open(f"machine_weekly_change.csv", 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(
        ['Plant Area ID', 'Date', 'Machine Weekly Amount'])
    for machine_result in machine_results:
        writer.writerow(
            [machine_result[0], machine_result[1], int(machine_result[2])])

# load
user_weekly_change = pd.read_csv("C:/Users/WorkAccount/PycharmProjects/pythonProject/venv/user_weekly_change.csv")
marker_weekly_change = pd.read_csv("C:/Users/WorkAccount/PycharmProjects/pythonProject/venv/marker_weekly_change.csv")
machine_weekly_change = pd.read_csv("C:/Users/WorkAccount/PycharmProjects/pythonProject/venv/machine_weekly_change.csv")


amm = company_areas_date_info.iloc[:,:5]
amm = pd.merge(amm,user_weekly_change,on=["Company ID","Date"],how="left")
amm = pd.merge(amm,marker_weekly_change,on=["Plant Area ID","Date"],how="left")
amm = pd.merge(amm,machine_weekly_change,on=["Plant Area ID","Date"],how="left")
# Change Date from yearweek format to week's Monday
amm['Date'] = amm['Date'].apply(lambda x: datetime.fromisocalendar(int(str(x)[0:4]), int(str(x)[4:6]), 1))

# output
amm.to_csv("./weekly_transaction_data.csv",encoding="utf-8",index=False)

# Close Database connection
conn.close()