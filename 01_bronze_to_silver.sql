-- Silver build: typed latest/current records, SCD2 history, and DQ quarantine.
-- Run in BigQuery Standard SQL. Batch dates are demo-effective dates; replace
-- these with source-provided effective dates when available.
DECLARE as_of_day DATE DEFAULT DATE '2026-10-28';

-- Current-state views/tables are rebuilt deterministically from Bronze.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` AS
SELECT a.* REPLACE (
  SAFE_CAST(a.CurrentBalance AS NUMERIC) AS CurrentBalance,
  SAFE_CAST(a.OpenDate AS DATE) AS OpenDate,
  SAFE_CAST(a.CloseDate AS DATE) AS CloseDate
)
FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.accounts` a
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY AccountID
  ORDER BY CASE _batch_id WHEN 'day0' THEN 0 WHEN 'day1' THEN 1 WHEN 'day2' THEN 2 ELSE -1 END DESC,
           _ingested_at DESC, _source_file DESC, _source_record_number DESC
) = 1;

CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.customers_current` AS
SELECT c.* REPLACE (
  SAFE_CAST(c.DateOfBirth AS DATE) AS DateOfBirth,
  SAFE_CAST(c.CreatedDate AS DATE) AS CreatedDate
)
FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.customers` c
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CustomerID
  ORDER BY CASE _batch_id WHEN 'day0' THEN 0 WHEN 'day1' THEN 1 WHEN 'day2' THEN 2 ELSE -1 END DESC,
           _ingested_at DESC, _source_file DESC, _source_record_number DESC
) = 1;

CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed` AS
SELECT t.* REPLACE (
  SAFE_CAST(t.TransactionDateTime AS TIMESTAMP) AS TransactionDateTime,
  SAFE_CAST(t.Amount AS NUMERIC) AS Amount,
  SAFE_CAST(t.BalanceAfter AS NUMERIC) AS BalanceAfter
)
FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.transactions` t
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY TransactionID
  ORDER BY CASE _batch_id WHEN 'day0' THEN 0 WHEN 'day1' THEN 1 WHEN 'day2' THEN 2 ELSE -1 END DESC,
           _ingested_at DESC
) = 1;

CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.loan_payments_typed` AS
SELECT p.* REPLACE (
  SAFE_CAST(p.DueDate AS DATE) AS DueDate,
  SAFE_CAST(p.PaymentDate AS DATE) AS PaymentDate,
  SAFE_CAST(p.AmountDue AS NUMERIC) AS AmountDue,
  SAFE_CAST(p.AmountPaid AS NUMERIC) AS AmountPaid
)
FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.loan_payments` p
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY PaymentID ORDER BY _ingested_at DESC
) = 1;

-- SCD2 snapshots: batch order gives effective date for this sample only.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.dim_account_scd2` AS
WITH versions AS (
  SELECT a.* REPLACE (
      SAFE_CAST(a.CurrentBalance AS NUMERIC) AS CurrentBalance,
      SAFE_CAST(a.OpenDate AS DATE) AS OpenDate,
      SAFE_CAST(a.CloseDate AS DATE) AS CloseDate
    ),
    CASE _batch_id WHEN 'day0' THEN DATE '2026-10-26'
                   WHEN 'day1' THEN DATE '2026-10-27'
                   WHEN 'day2' THEN DATE '2026-10-28' END AS effective_from
  FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.accounts` a
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY AccountID, _batch_id
    ORDER BY _ingested_at DESC, _source_file DESC, _source_record_number DESC
  ) = 1
), sequenced AS (
  SELECT *, LEAD(effective_from) OVER (PARTITION BY AccountID ORDER BY effective_from) AS next_from
  FROM versions
)
SELECT * EXCEPT(next_from),
       effective_from AS valid_from,
       COALESCE(DATE_SUB(next_from, INTERVAL 1 DAY), DATE '9999-12-31') AS valid_to,
       next_from IS NULL AS is_current
FROM sequenced;

CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.dim_customer_scd2` AS
WITH versions AS (
  SELECT c.* REPLACE (
      SAFE_CAST(c.DateOfBirth AS DATE) AS DateOfBirth,
      SAFE_CAST(c.CreatedDate AS DATE) AS CreatedDate
    ),
    CASE _batch_id WHEN 'day0' THEN DATE '2026-10-26'
                   WHEN 'day1' THEN DATE '2026-10-27'
                   WHEN 'day2' THEN DATE '2026-10-28' END AS effective_from
  FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.customers` c
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CustomerID, _batch_id
    ORDER BY _ingested_at DESC, _source_file DESC, _source_record_number DESC
  ) = 1
), sequenced AS (
  SELECT *, LEAD(effective_from) OVER (PARTITION BY CustomerID ORDER BY effective_from) AS next_from
  FROM versions
)
SELECT * EXCEPT(next_from),
       effective_from AS valid_from,
       COALESCE(DATE_SUB(next_from, INTERVAL 1 DAY), DATE '9999-12-31') AS valid_to,
       next_from IS NULL AS is_current
FROM sequenced;

-- DQ findings. Preserve source rows in Bronze; quarantine is an audit output.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.dq_quarantine` AS
SELECT 'CUSTOMERS' entity, CAST(CustomerID AS STRING) record_key,
       'CUSTOMER_EMAIL_MISSING' rule_id, 'Email is blank or null' reason,
       _batch_id, _source_file, _source_record_number, TO_JSON_STRING(c) record_json,
       CURRENT_TIMESTAMP() checked_at
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.customers_current` c
WHERE NULLIF(TRIM(Email), '') IS NULL
UNION ALL
SELECT 'CUSTOMERS', CAST(CustomerID AS STRING), 'CUSTOMER_PHONE_MISSING',
       'Phone is blank or null', _batch_id, _source_file, _source_record_number,
       TO_JSON_STRING(c), CURRENT_TIMESTAMP()
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.customers_current` c
WHERE NULLIF(TRIM(Phone), '') IS NULL
UNION ALL
SELECT 'TRANSACTIONS', CAST(t.TransactionID AS STRING), 'TRANSACTION_CURRENCY_MISMATCH',
       CONCAT('Transaction currency ', t.Currency, ' differs from account currency ', a.Currency),
       t._batch_id, t._source_file, t._source_record_number, TO_JSON_STRING(t), CURRENT_TIMESTAMP()
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed` t
JOIN `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a USING (AccountID)
WHERE UPPER(TRIM(t.Currency)) != UPPER(TRIM(a.Currency))
UNION ALL
SELECT 'ACCOUNTS', CAST(a.AccountID AS STRING), 'ACCOUNT_SORT_CODE_PREFIX_MISMATCH',
       'SortCode does not start with linked branch SortCodePrefix',
       a._batch_id, a._source_file, a._source_record_number, TO_JSON_STRING(a), CURRENT_TIMESTAMP()
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a
JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.branches` b USING (BranchID)
WHERE NOT STARTS_WITH(REGEXP_REPLACE(a.SortCode, r'[^0-9]', ''),
                      REGEXP_REPLACE(b.SortCodePrefix, r'[^0-9]', ''))
UNION ALL
SELECT 'ACCOUNTS', CAST(a.AccountID AS STRING), 'ACCOUNT_OVERDRAFT_LIMIT_EXCEEDED',
       'CurrentBalance is below the negative overdraft limit',
       a._batch_id, a._source_file, a._source_record_number, TO_JSON_STRING(a), CURRENT_TIMESTAMP()
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a
JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.account_types` t USING (AccountTypeID)
WHERE SAFE_CAST(t.OverdraftLimit AS NUMERIC) IS NOT NULL
  AND a.CurrentBalance < -SAFE_CAST(t.OverdraftLimit AS NUMERIC)
UNION ALL
SELECT 'TRANSACTIONS', CAST(TransactionID AS STRING), 'DUPLICATE_TRANSACTION_ID',
       'TransactionID occurs more than once in Bronze', _batch_id, _source_file,
       _source_record_number, TO_JSON_STRING(t), CURRENT_TIMESTAMP()
FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.transactions` t
QUALIFY COUNT(*) OVER (PARTITION BY TransactionID) > 1;

-- Referential integrity and parse checks as compact, queryable test results.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.silver_dataset.dq_summary` AS
SELECT 'accounts_to_customers_orphans' check_name,
       COUNTIF(c.CustomerID IS NULL) failed_rows
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a
LEFT JOIN `project-437b3d91-7f41-4318-ad5.silver_dataset.customers_current` c USING (CustomerID)
UNION ALL
SELECT 'accounts_to_branches_orphans', COUNTIF(b.BranchID IS NULL)
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a
LEFT JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.branches` b USING (BranchID)
UNION ALL
SELECT 'accounts_to_account_types_orphans', COUNTIF(acct_type.AccountTypeID IS NULL)
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a
LEFT JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.account_types` acct_type USING (AccountTypeID)
UNION ALL
SELECT 'transactions_to_accounts_orphans', COUNTIF(a.AccountID IS NULL)
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed` t
LEFT JOIN `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a USING (AccountID)
UNION ALL
SELECT 'transactions_to_transaction_types_orphans', COUNTIF(tt.TransactionTypeID IS NULL)
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed` t
LEFT JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.transaction_types` tt USING (TransactionTypeID)
UNION ALL
SELECT 'transactions_invalid_amount', COUNTIF(Amount IS NULL)
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed`
UNION ALL
SELECT 'transactions_invalid_datetime', COUNTIF(TransactionDateTime IS NULL)
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed`;

ASSERT (SELECT COUNTIF(failed_rows > 0) FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.dq_summary`) = 0
  AS 'Silver blocking DQ checks failed; inspect silver_dataset.dq_summary';
