# MySQLdbのインポート
import csv
import datetime
import time
import ast

import mysql.connector
from mysql.connector import Error

# 日付情報
running_date = datetime.date.today()
temp_running_date = '2023/08/08'

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
q_measure_length = f'''
SELECT
    id,data
FROM
    {db}.measure_lengths
order by id
'''

q_measurement_latest_date = f'''
SELECT
    DATE (max(created_at))
FROM
    {db}.measure_lengths
'''

def get_z_list(measure_data):
    l_z = []
    for i in range(8):
        l_z.append(ast.literal_eval(measure_data[1])[i]['z'])

    return l_z

conn = create_db_connection('localhost', 'root', pd, db)
results = read_query(conn, q_measure_length)

dict_z = {}
for result in results:
        if result[1].count('x')==8:
            dict_z[result[0]] = get_z_list(result)

for id,z_list in dict_z.items():
    print(id,z_list)

measurement_latest_date = read_query(conn,q_measurement_latest_date)
print(measurement_latest_date[0][0])


# with open(f"measure_length_data.csv", 'w', newline='', encoding='utf-8') as file:
#     writer = csv.writer(file)
#     writer.writerow(
#         ['id', 'coordinate amount','data'])
#     for result in results:
#         writer.writerow(
#             [result[0],result[1].count('x'),result[1]])

# 接続を閉じる
conn.close()


