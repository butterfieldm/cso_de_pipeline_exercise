CREATE OR REPLACE VIEW `cso-deng-pipeline.cso_exercise_bq_analysis.top_5_percent_customers_ltv` AS
WITH customer_ltv AS (
  SELECT
    customer_id,
    SUM(amount) AS lifetime_value
  FROM `cso-deng-pipeline.cso_exercise_bq_curated.transactions`
  GROUP BY customer_id
),
ranked_customers AS (
  SELECT
    customer_id,
    lifetime_value,
    NTILE(20) OVER (ORDER BY lifetime_value DESC) AS percentile
  FROM customer_ltv
)
SELECT
  c.customer_id,
  c.lifetime_value
FROM ranked_customers c
WHERE c.percentile = 1
ORDER BY c.lifetime_value DESC;
