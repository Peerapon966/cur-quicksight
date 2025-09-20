WITH customer_cte AS (
SELECT DISTINCT
    i."linked account id",
    i."partner customer code",
    i."partner name"
FROM "cur-quicksight".it_info i
),

-- Generate a list of months from Jan 2025 to next month from current date
date_cte AS (
    SELECT 
        format_datetime(
            date_add('month', seq, DATE '2025-01-01'),
            'yyyy-MM'
        ) AS yyyy_mm
    FROM UNNEST(
        sequence(
            0, 
            (year(current_date) - 2025) * 12 + month(current_date)
        )
    ) AS t(seq)
),

-- Extract customer account names and ids from data
customer_data_cte AS (
    SELECT DISTINCT
        line_item_usage_account_name,
        line_item_usage_account_id
    FROM "cur-quicksight".data
),

-- Create customer–month combinations
synnex_customer_cte AS (
    SELECT DISTINCT
        d.yyyy_mm,
        c.line_item_usage_account_name,
        i."linked account id",
        i."partner customer code",
        i."partner name"
    FROM date_cte d
    CROSS JOIN "cur-quicksight".it_info i
    LEFT JOIN customer_data_cte c
        ON i."linked account id" = c.line_item_usage_account_id
)

SELECT 
    d.line_item_usage_account_name,
    d.line_item_usage_account_id,
    CAST(d.line_item_unblended_cost AS DOUBLE) AS line_item_unblended_cost,
    0 AS "discount percent",
    CAST(0 AS DOUBLE) AS "discount usd",
    CAST(0 AS DOUBLE) AS "partner price",
    CAST(DATE_PARSE(d.line_item_usage_start_date, '%Y-%m-%dT%H:%i:%s.%fZ') AS DATE) AS line_item_usage_start_date,
    d.line_item_product_code,
    d.line_item_line_item_type,
    c."partner customer code",
    c."partner name"
FROM "cur-quicksight".data d
LEFT JOIN customer_cte c
    ON d.line_item_usage_account_id = c."linked account id"

UNION ALL

-- Join discounts with customer–month combinations
SELECT
    s.line_item_usage_account_name,
    s."linked account id"          AS line_item_usage_account_id,
    0.0                            AS line_item_unblended_cost,
    COALESCE(
        CAST(REPLACE(i."discount percent", '%', '') AS INTEGER),
        0
    ) AS "discount percent",
    COALESCE(
        CAST(REPLACE(i."discount usd", '$', '') AS DOUBLE),
        0
    ) AS "discount usd",
    COALESCE(
        CAST(REPLACE(i."partner price", '$', '') AS DOUBLE),
        0
    ) AS "partner price",
    CAST(date_parse(s.yyyy_mm, '%Y-%m') AS DATE) AS line_item_usage_start_date,
    'AmazonEC2' AS line_item_product_code,
    'Usage' AS line_item_line_item_type,
    s."partner customer code",
    s."partner name"
FROM synnex_customer_cte s
LEFT JOIN "cur-quicksight".it_info i
    ON  s."linked account id" = i."linked account id"
    AND s.yyyy_mm = i."invoice date"