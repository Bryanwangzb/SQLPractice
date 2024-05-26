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
where date(created_at) between '2024/04/01' - INTERVAL 1 MONTH AND '2024/04/01'
group by
    company_id

--日間平均ログイン数
-- 確認：ログイン回数を切り上げてよいか？
  SELECT
        cu.company_id,CEILING(count(*)/30)
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND date(cl.created_at) between '2024/04/01' - INTERVAL 1 MONTH AND '2024/04/01'
        AND cu.company_id=100
    group by cu.company_id


--日間平均ログイン数（前月比) 
WITH PRE_MONTH_LOGIN AS (
    SELECT
        cu.company_id,
        CEILING(COUNT(*) / 30.0) AS amount
    FROM
        company_logs AS cl
        LEFT JOIN company_users AS cu
            ON JSON_UNQUOTE(JSON_EXTRACT(cl.request_parameters, '$.email')) = cu.email
    WHERE
        cl.url = 'api/company/auth/login'
        AND cu.email NOT LIKE '%@brownreverse%'
        AND cl.created_at >= DATE_SUB('2024-03-01', INTERVAL 1 MONTH)
        AND cl.created_at < '2024-03-01'
        AND cu.company_id = 100
    GROUP BY
        cu.company_id
)
SELECT
    cu.company_id,
    CEILING(COUNT(*) / 30.0) / pml.amount AS login_ratio
FROM
    company_logs AS cl
    LEFT JOIN company_users AS cu
        ON JSON_UNQUOTE(JSON_EXTRACT(cl.request_parameters, '$.email')) = cu.email
    LEFT JOIN PRE_MONTH_LOGIN AS pml
        ON cu.company_id = pml.company_id
WHERE
    cl.url = 'api/company/auth/login'
    AND cu.email NOT LIKE '%@brownreverse%'
    AND cl.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH)
    AND cl.created_at < '2024-04-01'
    AND cu.company_id = 100
GROUP BY
    cu.company_id, pml.amount;

--週間平均ログイン数
 SELECT
        cu.company_id,CEILING(count(*)/4)
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND date(cl.created_at) between '2024/04/01' - INTERVAL 1 MONTH AND '2024/04/01'
        AND cu.company_id=100
    group by cu.company_id


--週間平均ログイン数（前月比）
WITH PRE_MONTH_LOGIN AS (
    SELECT
        cu.company_id,
        CEILING(COUNT(*) / 4) AS amount
    FROM
        company_logs AS cl
        LEFT JOIN company_users AS cu
            ON JSON_UNQUOTE(JSON_EXTRACT(cl.request_parameters, '$.email')) = cu.email
    WHERE
        cl.url = 'api/company/auth/login'
        AND cu.email NOT LIKE '%@brownreverse%'
        AND cl.created_at >= DATE_SUB('2024-03-01', INTERVAL 1 MONTH)
        AND cl.created_at < '2024-03-01'
        AND cu.company_id = 100
    GROUP BY
        cu.company_id
)
SELECT
    cu.company_id,
    CEILING(COUNT(*) / 4) / pml.amount AS login_ratio
FROM
    company_logs AS cl
    LEFT JOIN company_users AS cu
        ON JSON_UNQUOTE(JSON_EXTRACT(cl.request_parameters, '$.email')) = cu.email
    LEFT JOIN PRE_MONTH_LOGIN AS pml
        ON cu.company_id = pml.company_id
WHERE
    cl.url = 'api/company/auth/login'
    AND cu.email NOT LIKE '%@brownreverse%'
    AND cl.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH)
    AND cl.created_at < '2024-04-01'
    AND cu.company_id = 100
GROUP BY
    cu.company_id, pml.amount;

--月間ログイン人数
SELECT
        cu.company_id,count(*)
    FROM
        company_logs AS cl 
        LEFT OUTER JOIN company_users AS cu 
            ON JSON_UNQUOTE(json_extract(cl.request_parameters, '$.email')) = cu.email 
    WHERE
        cl.url = 'api/company/auth/login' 
        AND cu.email NOT LIKE '%@brownreverse%' 
        AND date(cl.created_at) between '2024/04/01' - INTERVAL 1 MONTH AND '2024/04/01'
        AND cu.company_id=100
    group by cu.company_id

--月間平均ログイン数（前月比）
WITH PRE_MONTH_LOGIN AS (
    SELECT
        cu.company_id,
        COUNT(*)
    FROM
        company_logs AS cl
        LEFT JOIN company_users AS cu
            ON JSON_UNQUOTE(JSON_EXTRACT(cl.request_parameters, '$.email')) = cu.email
    WHERE
        cl.url = 'api/company/auth/login'
        AND cu.email NOT LIKE '%@brownreverse%'
        AND cl.created_at >= DATE_SUB('2024-03-01', INTERVAL 1 MONTH)
        AND cl.created_at < '2024-03-01'
        AND cu.company_id = 100
    GROUP BY
        cu.company_id
)
SELECT
    cu.company_id,
    CEILING(COUNT(*) / 4) / pml.amount AS login_ratio
FROM
    company_logs AS cl
    LEFT JOIN company_users AS cu
        ON JSON_UNQUOTE(JSON_EXTRACT(cl.request_parameters, '$.email')) = cu.email
    LEFT JOIN PRE_MONTH_LOGIN AS pml
        ON cu.company_id = pml.company_id
WHERE
    cl.url = 'api/company/auth/login'
    AND cu.email NOT LIKE '%@brownreverse%'
    AND cl.created_at >= DATE_SUB('2024-04-01', INTERVAL 1 MONTH)
    AND cl.created_at < '2024-04-01'
    AND cu.company_id = 100
GROUP BY
    cu.company_id, pml.amount;

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
    AND pg.created_at < '2024-04-01'

