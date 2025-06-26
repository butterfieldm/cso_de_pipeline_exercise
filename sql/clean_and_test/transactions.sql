DECLARE missing_fields INT64;
DECLARE duplicate_count INT64;
DECLARE future_transaction_dttm INT64;

-- Check for missing required fields
SET missing_fields = (
  SELECT COUNT(*) 
  FROM `cso-deng-pipeline.cso_exercise_bq_staging.transactions` 
  WHERE transaction_id IS NULL OR
        customer_id IS NULL OR 
        transaction_dttm IS NULL OR
        amount IS NULL
);

-- Check for duplicate transaction_id (assuming transaction_id should be unique)
SET duplicate_count = (
  SELECT COUNT(*)
  FROM (
    SELECT transaction_id, COUNT(*) AS cnt
    FROM `cso-deng-pipeline.cso_exercise_bq_staging.transactions` 
    GROUP BY transaction_id
    HAVING cnt > 1
  )
);

-- Check for transaction_dttm >= tomorrow
SET future_transaction_dttm = (
  SELECT COUNT(*)
  FROM `cso-deng-pipeline.cso_exercise_bq_staging.transactions` 
  WHERE transaction_dttm >= TIMESTAMP(CURRENT_DATE() + 1)
);

-- Fail on any check
IF missing_fields > 0 THEN
  RAISE USING MESSAGE = FORMAT("Validation failed: %d missing required fields", missing_fields);
END IF;

IF duplicate_count > 0 THEN
  RAISE USING MESSAGE = FORMAT("Validation failed: %d duplicate transaction_ids", duplicate_count);
END IF;

IF future_transaction_dttm > 0 THEN
  RAISE USING MESSAGE = FORMAT("Validation failed: %d rows have future transaction_dttm dates", future_transaction_dttm);
END IF;

-- If all checks pass, clean the data and promote to curated layer
CREATE OR REPLACE TABLE `cso-deng-pipeline.cso_exercise_bq_curated.transactions` AS
SELECT
  transaction_id,
  customer_id,
  transaction_dttm,
  amount,
  CASE  
    WHEN LOWER(TRIM(transaction_type)) = 'deposit' THEN 'D'
    WHEN LOWER(TRIM(transaction_type)) = 'standing order' THEN 'SO'
    WHEN LOWER(TRIM(transaction_type)) = 'direct debit' THEN 'DD'
    WHEN LOWER(TRIM(transaction_type)) = 'debit card' THEN 'DC'
    ELSE TRIM(transaction_type)
  END AS transaction_type,
  UPPER(TRIM(merchant_name)) AS merchant_name,
  UPPER(TRIM(category)) AS category,
  balance_after
FROM `cso-deng-pipeline.cso_exercise_bq_staging.transactions`;
