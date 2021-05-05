WITH
  visit_log AS(
  SELECT
    cust_code,
    DATE_TRUNC(PARSE_DATE('%Y%m%d',
        CAST(shop_date AS STRING)), MONTH) AS visit_date_fix
  FROM
    `bads7105-crm-312814.supermarket_dataset.transitions`
  WHERE
    cust_code IS NOT NULL
    AND cust_code != ''
  GROUP BY
    1,
    2
  ORDER BY
    1,
    2),
  Time_lapse AS(
  SELECT
    cust_code,
    visit_date_fix,
    LEAD(visit_date_fix, 1) OVER (PARTITION BY cust_code ORDER BY cust_code, visit_date_fix) AS visit_date_lead,
    LAG(visit_date_fix, 1) OVER (PARTITION BY cust_code ORDER BY cust_code, visit_date_fix) AS visit_date_lag
  FROM
    visit_log),
  Time_diff_calculated AS(
  SELECT
    cust_code,
    visit_date_fix,
    visit_date_lead,
    visit_date_lag,
    DATE_DIFF(visit_date_lead, visit_date_fix, MONTH) AS time_diff_lead,
    DATE_DIFF(visit_date_fix, visit_date_lag, MONTH) AS time_diff_lag
  FROM
    time_lapse
  ORDER BY
    1,
    2),
  Custs_categorized_lag AS(
  SELECT
    cust_code,
    visit_date_fix,
    visit_date_lag,
    time_diff_lag,
    CASE
      WHEN time_diff_lag =1 THEN 'Repeat Customer'
      WHEN time_diff_lag >1 THEN 'Reactivated Customer'
      WHEN time_diff_lag IS NULL THEN 'New Customer'
    ELSE
    'n/a'
  END
    AS cust_type
  FROM
    Time_diff_calculated),
  Custs_categorized_lead AS(
  SELECT
    cust_code,
    DATE_ADD(visit_date_fix, INTERVAL +1 MONTH) AS visit_date_fix,
    visit_date_lead,
    time_diff_lead,
    CASE
      WHEN time_diff_lead =1 THEN 'Repeat Customer'
    ELSE
    'Churn Customer'
  END
    AS cust_type
  FROM
    Time_diff_calculated),
  Summary AS(
  SELECT
    cust_code,
    visit_date_fix,
    cust_type
  FROM
    Custs_categorized_lag
  UNION DISTINCT
  SELECT
    cust_code,
    visit_date_fix,
    cust_type
  FROM
    Custs_categorized_lead
  WHERE
    cust_type = 'Churn Customer'
  ORDER BY
    1,
    2)
SELECT
  visit_date_fix,
  COUNT(DISTINCT
    CASE
      WHEN cust_type = 'New Customer' THEN cust_code
  END
    ) AS New_Customer,
  COUNT(DISTINCT
    CASE
      WHEN cust_type = 'Repeat Customer' THEN cust_code
  END
    ) AS Repeat_Customer,
  COUNT(DISTINCT
    CASE
      WHEN cust_type = 'Reactivated Customer' THEN cust_code
  END
    ) AS Reactivated_Customer,
  -COUNT(DISTINCT
    CASE
      WHEN cust_type = 'Churn Customer' THEN cust_code
  END
    ) AS Churn_Customer
FROM
  Summary
WHERE
  visit_date_fix != '2008-08-01'
GROUP BY
  visit_date_fix
ORDER BY
  visit_date_fix;
