--ユーザー登録数

select
    company_id, count(*) as user_amount 
from
    company_users 
group by
    company_id

--新規登録ユーザー数
select
    company_id
    , count(*) as user_amount 
from
    company_users 
AND DATE (created_at) >=('2024/04/01' - INTERVAL 1 MONTH) AND DATE (created_at)<'2024/04/01' 
group by
    company_id

--日間平均ログイン人数
WITH daiy_login_temp AS ( 
    SELECT
        cu.company_id AS company_id
        , count(DISTINCT DATE (cl.created_at)) AS login_count 
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
          AND DATE (cl.created_at) >=('2024/04/01' - INTERVAL 1 MONTH) AND DATE (cl.created_at)<'2024/04/01' 
        AND cu.company_id = 100 
    GROUP BY
        cu.id
        , DATE (cl.created_at)
) 
SELECT
    company_id
    , ceiling(sum(login_count) /31)
FROM
    daiy_login_temp


--日間平均ログイン数（前月比) 
WITH daily_login_temp AS (
    SELECT
        cu.company_id AS company_id,
        DATE(cl.created_at) AS login_date,
        COUNT(DISTINCT DATE(cl.created_at)) AS login_count 
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(JSON_EXTRACT(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND cu.company_id = 100 
    GROUP BY
        cu.company_id,
        cu.id,
        DATE(cl.created_at)
),
login_counts as (
    SELECT
        company_id,
        SUM(CASE 
            WHEN login_date >=('2024/03/01' - INTERVAL 1 MONTH) AND login_date < '2024/03/01' THEN login_count
            ELSE 0 
        END) AS previous_month_login_count,
        SUM(CASE 
            WHEN login_date >= ( '2024/04/01' - INTERVAL 1 MONTH) AND login_date < '2024/04/01' THEN login_count
            ELSE 0 
        END) AS current_month_login_count
    FROM
        daily_login_temp
    GROUP BY
        company_id
)
SELECT
    company_id,
    ceiling(previous_month_login_count/29) as previous_avg,
    ceiling(current_month_login_count/31) as current_avg,
    CASE 
        WHEN current_month_login_count = 0 THEN NULL 
        ELSE  ceiling(current_month_login_count/31)/ceiling(previous_month_login_count/29) 
    END AS login_count_ratio
FROM
    login_counts;


--週間平均ログイン数
WITH week_login_temp AS ( 
    SELECT
        cu.company_id AS company_id
        ,  yearweek (cl.created_at) as created_yearweek
        , count(DISTINCT yearweek (cl.created_at)) AS login_count 
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND DATE (cl.created_at) >= ( '2024/04/01' - INTERVAL 1 MONTH) AND DATE (cl.created_at) < '2024/04/01' 
        AND cu.company_id = 100 
    GROUP BY
        cu.id
        , yearweek (cl.created_at)
) 
SELECT
    company_id
    , ceiling(sum(login_count)/4)
FROM
    week_login_temp
    

--週間平均ログイン数（前月比）
WITH year_week_current_login AS ( 
    WITH week_login_temp AS ( 
        SELECT
            cu.company_id AS company_id
            , cu.id
            , yearweek(cl.created_at) AS created_yearweek
            , count(DISTINCT yearweek(cl.created_at)) AS login_count 
        FROM
            company_logs AS cl 
            LEFT OUTER JOIN company_users AS cu 
                ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
        WHERE
            cl.url = 'api/company/auth/login' 
            AND cu.email NOT LIKE '%@brownreverse%' 
            AND DATE (cl.created_at) >= '2024/04/01' - INTERVAL 1 MONTH AND DATE (cl.created_at) <'2024/04/01' 
            AND cu.company_id = 100 
        GROUP BY
            cu.company_id
            , cu.id
            , yearweek(cl.created_at)
    ) 
    SELECT
        company_id
        , sum(login_count) AS temp_result
        , ceiling(sum(login_count) / 4) AS RESULT 
    FROM
        week_login_temp
) 
, year_week_previous_login AS ( 
    WITH week_login_temp AS ( 
        SELECT
            cu.company_id AS company_id
            , cu.id
            , yearweek(cl.created_at) AS created_yearweek
            , count(DISTINCT yearweek(cl.created_at)) AS login_count 
        FROM
            company_logs AS cl 
            LEFT OUTER JOIN company_users AS cu 
                ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
        WHERE
            cl.url = 'api/company/auth/login' 
            AND cu.email NOT LIKE '%@brownreverse%' 
            AND DATE (cl.created_at) >= ('2024/03/01' - INTERVAL 1 MONTH) AND date(cl.created_at)<'2024/03/01' 
            AND cu.company_id = 100 
        GROUP BY
            cu.company_id
            , cu.id
            , yearweek(cl.created_at)
    ) 
    SELECT
        company_id
        , sum(login_count) AS temp_result
        , ceiling(sum(login_count) / 4) AS RESULT 
    FROM
        week_login_temp
) 
SELECT
    ywc.temp_result
    , ywp.temp_result
    , ywc.temp_result / ywp.temp_result 
FROM
    year_week_current_login AS ywc 
    LEFT OUTER JOIN year_week_previous_login AS ywp 
        ON ywc.company_id = ywp.company_id


--月間ログイン人数
WITH month_login_temp AS ( 
    SELECT
        cu.company_id AS company_id
        , yearweek(cl.created_at) AS created_yearweek
        , count(DISTINCT MONTH (cl.created_at)) AS login_count 
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND DATE (cl.created_at) >= ('2024/04/01' - INTERVAL 1 MONTH) 
        AND DATE (cl.created_at) < '2024/04/01' 
        AND cu.company_id = 100 
    GROUP BY
        cu.id
        , MONTH (cl.created_at)
) 
SELECT
    company_id
    , sum(login_count)
FROM
    month_login_temp


--月間平均ログイン数（前月比）
WITH year_month_current_login AS ( 
 with month_login_temp as(
    SELECT
        cu.company_id AS company_id
        , yearweek(cl.created_at) AS created_yearweek
        , count(DISTINCT MONTH (cl.created_at)) AS login_count 
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND DATE (cl.created_at) >= ('2024/04/01' - INTERVAL 1 MONTH) 
        AND DATE (cl.created_at) < '2024/04/01' 
        AND cu.company_id = 100 
    GROUP BY
        cu.id
        , MONTH (cl.created_at)
) 
SELECT
    company_id
    , sum(login_count) as login_count
FROM
    month_login_temp
),   
year_month_previous_login AS ( 
    with month_login_temp as(
    SELECT
        cu.company_id AS company_id
        , yearweek(cl.created_at) AS created_yearweek
        , count(DISTINCT MONTH (cl.created_at)) AS login_count 
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND DATE (cl.created_at) >= ('2024/03/01' - INTERVAL 1 MONTH) 
        AND DATE (cl.created_at) < '2024/03/01' 
        AND cu.company_id = 100 
    GROUP BY
        cu.id
        , MONTH (cl.created_at)
) 
SELECT
    company_id
    , sum(login_count) as login_count
FROM
    month_login_temp)
select
     ymc.company_id,
     ymc.login_count/ymp.login_count
from
    year_month_current_login as ymc
left outer join
    year_month_previous_login as ymp
on
    ymc.company_id = ymp.company_id

--マーカー数（合計）
with company_area_info as (
SELECT
    com.id AS company_id
    , com.name as company_name
    , pa.id AS area_id 
    , pa.name as area_name
FROM
    companies AS com 
    LEFT OUTER JOIN plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN plant_areas AS pa 
        ON pla.id = pa.plant_id)
select cai.company_id,count(*) from markers as mk
left outer join
    company_area_info as cai
on
    mk.plant_area_id = cai.area_id
where cai.company_id=100     
AND mk.created_at < '2024-04-01'

--新規マーカー数
WITH company_area_info AS (
SELECT
    com.id AS company_id
    , com.name AS company_name
    , pa.id AS area_id 
    , pa.name AS area_name
FROM
    companies AS com 
    LEFT OUTER JOIN plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN plant_areas AS pa 
        ON pla.id = pa.plant_id)
SELECT cai.company_id,count(*) FROM markers AS mk
LEFT OUTER JOIN
    company_area_info AS cai
ON
    mk.plant_area_id = cai.area_id
WHERE cai.company_id=100     
AND mk.created_at < '2024-04-01'
AND mk.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH)
AND mk.created_at < '2024-04-01'

--機番登録数（合計）
WITH company_area_info AS (
SELECT
    com.id AS company_id
    , com.name AS company_name
    , pa.id AS area_id 
    , pa.name AS area_name
FROM
    companies AS com 
    LEFT OUTER JOIN plants AS pla 
        ON com.id = pla.company_id 
    LEFT OUTER JOIN plant_areas AS pa 
        ON pla.id = pa.plant_id)
SELECT 
    coalesce(cai.company_id,'100'),count(*) 
FROM 
    assets AS ast
LEFT OUTER JOIN
    company_area_info AS cai
ON
    ast.plant_area_id = cai.area_id
WHERE cai.company_id=100     
AND ast.created_at < '2024-04-01'

--新規機番登録数
WITH company_area_info AS ( 
    SELECT
        com.id AS company_id
        , com.name AS company_name
        , pa.id AS area_id
        , pa.name AS area_name 
    FROM
        companies AS com 
        LEFT OUTER JOIN plants AS pla 
            ON com.id = pla.company_id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON pla.id = pa.plant_id
) 
SELECT
    coalesce(cai.company_id,'100')
    , count(*) 
FROM
    assets AS ast 
    LEFT OUTER JOIN company_area_info AS cai 
        ON ast.plant_area_id = cai.area_id 
WHERE
    cai.company_id = 100 
    AND ast.created_at < '2024-04-01' 
    AND ast.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH) 
    AND ast.created_at < '2024-04-01'

--測長数（合計）
WITH company_area_info AS ( 
    SELECT
        com.id AS company_id
        , com.name AS company_name
        , pa.id AS area_id
        , pa.name AS area_name 
    FROM
        companies AS com 
        LEFT OUTER JOIN plants AS pla 
            ON com.id = pla.company_id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON pla.id = pa.plant_id
) 
SELECT
    coalesce(cai.company_id,'100')
    , count(*) 
FROM
    measure_lengths AS ms 
    LEFT OUTER JOIN company_area_info AS cai 
        ON ms.plant_area_id = cai.area_id 
WHERE
    cai.company_id = 100 
    AND ms.created_at < '2024-04-01' 

--新規測長数
WITH company_area_info AS ( 
    SELECT
        com.id AS company_id
        , com.name AS company_name
        , pa.id AS area_id
        , pa.name AS area_name 
    FROM
        companies AS com 
        LEFT OUTER JOIN plants AS pla 
            ON com.id = pla.company_id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON pla.id = pa.plant_id
) 
SELECT
    coalesce(cai.company_id,'100')
    , count(*) 
FROM
    measure_lengths AS ms 
    LEFT OUTER JOIN company_area_info AS cai 
        ON ms.plant_area_id = cai.area_id 
WHERE
    cai.company_id = 100 
    AND ms.created_at < '2024-04-01' 
    AND ms.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH) 
    AND ms.created_at < '2024-04-01'

--空間シミュレーション数（合計）

WITH company_area_info AS ( 
    SELECT
        com.id AS company_id
        , com.name AS company_name
        , pa.id AS area_id
        , pa.name AS area_name 
    FROM
        companies AS com 
        LEFT OUTER JOIN plants AS pla 
            ON com.id = pla.company_id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON pla.id = pa.plant_id
) 
SELECT
    coalesce(cai.company_id,'100')
    , count(*) 
FROM
    plant_area_objects AS pao 
    LEFT OUTER JOIN company_area_info AS cai 
        ON pao.plant_area_id = cai.area_id 
WHERE
    cai.company_id = 100 
    AND pao.created_at < '2024-04-01' 


--新規空間シミュレーション数
WITH company_area_info AS ( 
    SELECT
        com.id AS company_id
        , com.name AS company_name
        , pa.id AS area_id
        , pa.name AS area_name 
    FROM
        companies AS com 
        LEFT OUTER JOIN plants AS pla 
            ON com.id = pla.company_id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON pla.id = pa.plant_id
) 
SELECT
    coalesce(cai.company_id,'100')
    , count(*) 
FROM
    plant_area_objects AS pao 
    LEFT OUTER JOIN company_area_info AS cai 
        ON pao.plant_area_id = cai.area_id 
WHERE
    cai.company_id = 100 
    AND pao.created_at < '2024-04-01' 
    AND pao.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH) 
    AND pao.created_at < '2024-04-01'

--配管登録数（合計）
WITH company_area_info AS ( 
    SELECT
        com.id AS company_id
        , com.name AS company_name
        , pa.id AS area_id
        , pa.name AS area_name 
    FROM
        companies AS com 
        LEFT OUTER JOIN plants AS pla 
            ON com.id = pla.company_id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON pla.id = pa.plant_id
) 
SELECT
    coalesce(cai.company_id,'100')
    , count(*) 
FROM
    pipe_groups AS pg 
    LEFT OUTER JOIN company_area_info AS cai 
        ON pg.plant_area_id = cai.area_id 
WHERE
    cai.company_id = 100 
    AND pg.created_at < '2024-04-01' 


--新規配管登録数

WITH company_area_info AS ( 
    SELECT
        com.id AS company_id
        , com.name AS company_name
        , pa.id AS area_id
        , pa.name AS area_name 
    FROM
        companies AS com 
        LEFT OUTER JOIN plants AS pla 
            ON com.id = pla.company_id 
        LEFT OUTER JOIN plant_areas AS pa 
            ON pla.id = pa.plant_id
) 
SELECT
    coalesce(cai.company_id,'100')
    , count(*) 
FROM
    pipe_groups AS pg 
    LEFT OUTER JOIN company_area_info AS cai 
        ON pg.plant_area_id = cai.area_id 
WHERE
    cai.company_id = 100 
    AND pg.created_at < '2024-04-01' 
    AND pg.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH) 

