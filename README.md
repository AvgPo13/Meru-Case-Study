# Meru-Case-Study: Data Cleanup SQL Queries
# Budget Cleanup
-- STEP 1: Filter out metadata and invalid rows from the raw budget
-- - Remove header rows where C1 = 'Customer Code'
-- - Ensure C2 (Item Code) is numeric

CREATE OR REPLACE TABLE raw_budget_clean AS
SELECT *
FROM raw_budget
WHERE TRY_TO_NUMBER(C2) IS NOT NULL
  AND UPPER(C1) != 'CUSTOMER CODE';


-- STEP 2: Rename metadata columns for clarity
ALTER TABLE raw_budget_clean 
  RENAME COLUMN C1 TO customer_code,
  RENAME COLUMN C2 TO item_code,
  RENAME COLUMN C3 TO business,
  RENAME COLUMN C4 TO channel,
  RENAME COLUMN C5 TO branded_pl,
  RENAME COLUMN C6 TO item_group;


-- STEP 3: Pivot monthly CASE and DOLLAR columns to long format
-- - C7–C18 contain CASES for Jan–Dec 2021
-- - C19–C30 contain DOLLAR values for Jan–Dec 2021
-- - Each UNION ALL creates one row per month per record

CREATE OR REPLACE TABLE cleaned_budget AS
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Jan-21' AS month, CAST(C7  AS INT) AS cases, CAST(C19 AS INT) AS dollar
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Feb-21', CAST(C8  AS INT), CAST(C20 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Mar-21', CAST(C9  AS INT), CAST(C21 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Apr-21', CAST(C10 AS INT), CAST(C22 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'May-21', CAST(C11 AS INT), CAST(C23 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Jun-21', CAST(C12 AS INT), CAST(C24 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Jul-21', CAST(C13 AS INT), CAST(C25 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Aug-21', CAST(C14 AS INT), CAST(C26 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Sep-21', CAST(C15 AS INT), CAST(C27 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Oct-21', CAST(C16 AS INT), CAST(C28 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Nov-21', CAST(C17 AS INT), CAST(C29 AS INT)
FROM raw_budget_clean

UNION ALL
SELECT customer_code, item_code, business, channel, branded_pl, item_group,
       'Dec-21', CAST(C18 AS INT), CAST(C30 AS INT)
FROM raw_budget_clean;


-- STEP 4: Preview the cleaned and normalized budget data
SELECT *
FROM cleaned_budget;

# Case Weight Cleanup
-- STEP 1: View raw case weight data
-- - Used for validation and understanding of structure

SELECT *
FROM cs.public.raw_caseweights;


-- STEP 2: Create cleaned case weight table
-- - Trim whitespace from item numbers
-- - Convert case weight to numeric with two decimal places
-- - Exclude invalid item numbers that contain or equal '-1'

CREATE OR REPLACE TABLE cleaned_caseweights AS
SELECT 
    TRIM(item_no) AS item_code,                          -- standardize item codes
    CAST(case_weight_lb AS NUMBER(10,2)) AS case_weight_lb  -- preserve decimal precision
FROM raw_caseweights
WHERE TRIM(item_no) NOT LIKE '%-1%'                      -- exclude partial '-1' values
  AND TRIM(item_no) != '-1';                             -- exclude exact '-1' codes


-- STEP 3: Preview the cleaned case weights
SELECT *
FROM cleaned_caseweights;

# Historical Sales Cleanup
-- Step 1: View raw historical sales data.
SELECT *
FROM cs.public.raw_historical_sales;

-- Step 2: Clean and prepare historical sales data for combination.
-- Rename fields for consistency.
-- Cast item and customer codes as text to preserve formatting.
-- Clean and convert numeric fields for quantity, dollar, and weight.
-- Create a readable month label for time-based analysis.

CREATE OR REPLACE TABLE cleaned_historical_sales AS
SELECT
    business_line AS business,                                           -- Standardize business field name.

    CAST(item_code AS VARCHAR) AS item_code,                             -- Preserve item codes as text.
    CAST(customer_code AS VARCHAR) AS customer_code,                     -- Preserve customer codes as text.

    TRY_TO_NUMBER(REPLACE(quantity, ',', '')) AS quantity,               -- Convert quantity to integer.
    TRY_TO_NUMBER(REPLACE(REPLACE(total_amt, '$', ''), ',', '')) AS dollar,  -- Clean and convert dollar amount.

    channel,                                                             -- Retain sales channel info.
    item_group,                                                          -- Retain item group info.
    branded_pl,                                                          -- Retain brand flag.
    states,                                                              -- Retain regional information.

    TRY_TO_NUMBER(REPLACE(lbspercase, ',', '')) AS lbspercase,          -- Clean and convert weight per case.
    TRY_TO_NUMBER(REPLACE(lbs, ',', '')) AS lbs,                         -- Clean and convert total pounds.

    TO_CHAR(TO_DATE(year || '-' || month || '-01', 'YYYY-MM-DD'), 'MON-YY') AS month_label  -- Format month.

FROM cs.public.raw_historical_sales;

-- Step 3: Preview cleaned historical sales data.
SELECT *
FROM cleaned_historical_sales;

# Budget and Case Weight Join
-- Step 1: Create cleaned budget table with case weights.
-- Join budget data with case weights by item code.
-- Calculate pounds (LBS) as cases multiplied by case weight.
-- Round LBS to two decimal places for accuracy.

CREATE OR REPLACE TABLE cleaned_budget_with_weight AS
SELECT 
    b.customer_code,                                   -- Customer identifier.
    b.item_code,                                       -- Product identifier.
    b.business,                                        -- Business line.
    b.channel,                                         -- Sales channel.
    b.branded_pl,                                      -- Brand flag.
    b.item_group,                                      -- Product grouping.
    b.month,                                           -- Time period.
    b.cases,                                           -- Number of cases.
    b.dollar,                                          -- Dollar amount.
    cw.case_weight_lb,                                 -- Case weight from reference table.
    ROUND(b.cases * cw.case_weight_lb, 2) AS lbs       -- Total pounds, rounded to 2 decimals.
FROM cleaned_budget b
LEFT JOIN cleaned_caseweights cw
    ON b.item_code = cw.item_code;

-- Step 2: Preview the cleaned budget with weight data.
SELECT *
FROM cleaned_budget_with_weight;

# Final Table
-- Step 1: Combine historical sales and budget forecast into a unified table.

CREATE OR REPLACE TABLE final_combined_data AS

-- Part 1: Historical sales (actuals).
SELECT
    business,
    item_code,
    customer_code,
    channel,
    item_group,
    CASE 
        WHEN channel = 'Retail' THEN 'Branded'
        WHEN channel = 'Retail Private Label' THEN 'PL'
        WHEN branded_pl IN ('PL', 'Private Label') THEN 'PL'
        ELSE branded_pl
    END AS branded_pl,
    month_label AS month,
    quantity AS cases,
    dollar AS amount,
    lbspercase AS weight_per_case,
    lbs AS total_weight
FROM cleaned_historical_sales
WHERE business IS NOT NULL
  AND item_code IS NOT NULL
  AND customer_code IS NOT NULL
  AND channel IS NOT NULL
  AND item_group IS NOT NULL
  AND month_label IS NOT NULL
  AND quantity IS NOT NULL
  AND dollar IS NOT NULL
  AND lbspercase IS NOT NULL
  AND lbs IS NOT NULL

UNION ALL

-- Part 2: Budget with weights (forecast).
SELECT
    business,
    item_code,
    customer_code,
    channel,
    item_group,
    CASE 
        WHEN channel = 'Retail' THEN 'Branded'
        WHEN channel = 'Retail Private Label' THEN 'PL'
        WHEN branded_pl IN ('PL', 'Private Label') THEN 'PL'
        ELSE branded_pl
    END AS branded_pl,
    month AS month,
    cases,
    dollar AS amount,
    case_weight_lb AS weight_per_case,
    lbs AS total_weight
FROM cleaned_budget_with_weight
WHERE business IS NOT NULL
  AND item_code IS NOT NULL
  AND customer_code IS NOT NULL
  AND channel IS NOT NULL
  AND item_group IS NOT NULL
  AND month IS NOT NULL
  AND cases IS NOT NULL
  AND dollar IS NOT NULL
  AND case_weight_lb IS NOT NULL
  AND lbs IS NOT NULL;

-- Step 2: Preview the final combined dataset.
SELECT *
FROM final_combined_data;
