CREATE OR REPLACE TABLE `cso-deng-pipeline.cso_exercise_bq_analysis.customer_monthly_spend`
PARTITION BY month_date
CLUSTER BY customer_id AS
SELECT
  a.customer_id,
  CONCAT(b.first_name, ' ', b.last_name) AS full_name,
  DATE_TRUNC(a.transaction_dttm, MONTH) AS month_date, 
  SUM(a.amount) AS total_monthly_spend,
  AVG(a.amount) AS avg_transaction_amount
FROM `cso-deng-pipeline.cso_exercise_bq_curated.transactions` a
LEFT JOIN `cso-deng-pipeline.cso_exercise_bq_curated.customers` b
    ON a.customer_id=b.customer_id
GROUP BY customer_id, full_name, month_date;
