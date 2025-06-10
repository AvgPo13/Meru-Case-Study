# Meru-Case-Study: Budget Cleanup
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
