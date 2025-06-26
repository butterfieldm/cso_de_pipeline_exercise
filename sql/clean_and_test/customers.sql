DECLARE missing_fields INT64;
DECLARE duplicate_count INT64;
DECLARE future_valid_from INT64;
DECLARE invalid_dob INT64;

-- Check for missing required fields
SET missing_fields = (
  SELECT COUNT(*) 
  FROM `cso-deng-pipeline.cso_exercise_bq_staging.customers` 
  WHERE customer_id IS NULL OR 
        first_name IS NULL OR
        last_name IS NULL OR
        date_of_birth IS NULL OR
        postcode IS NULL OR
        valid_from_dttm IS NULL OR
        valid_to_dttm IS NULL
);

-- Check for duplicate customer_id
SET duplicate_count = (
  SELECT COUNT(*)
  FROM (
    SELECT customer_id, COUNT(*) AS cnt
    FROM `cso-deng-pipeline.cso_exercise_bq_staging.customers` 
    GROUP BY customer_id
    HAVING cnt > 1
  )
);

-- Check for valid_from >= tomorrow
SET future_valid_from = (
  SELECT COUNT(*)
  FROM `cso-deng-pipeline.cso_exercise_bq_staging.customers` 
  WHERE valid_from_dttm >= TIMESTAMP(CURRENT_DATE() + 1)
);

-- Check for valid date_of_birth (older than today)
SET invalid_dob = (
  SELECT COUNT(*)
  FROM `cso-deng-pipeline.cso_exercise_bq_staging.customers` 
  WHERE date_of_birth > CURRENT_DATE()
);

-- Fail on any check
IF missing_fields > 0 THEN
  RAISE USING MESSAGE = FORMAT("Validation failed: %d missing required fields", missing_fields);
END IF;

IF duplicate_count > 0 THEN
  RAISE USING MESSAGE = FORMAT("Validation failed: %d duplicate customer_ids", duplicate_count);
END IF;

IF future_valid_from > 0 THEN
  RAISE USING MESSAGE = FORMAT("Validation failed: %d rows have future valid_from dates", future_valid_from);
END IF;

IF invalid_dob > 0 THEN
  RAISE USING MESSAGE = FORMAT("Validation failed: %d rows have invalid future date_of_birth values", invalid_dob);
END IF;

-- If all checks pass, clean and promote to curated layer
CREATE OR REPLACE TABLE `cso-deng-pipeline.cso_exercise_bq_curated.customers` AS
SELECT
  customer_id,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  TRIM(email) AS email,
  date_of_birth,
  TRIM(REPLACE(phone, '+', '')) AS phone,
  city,
  postcode,
  valid_from_dttm,
  valid_to_dttm
FROM `cso-deng-pipeline.cso_exercise_bq_staging.customers`;
