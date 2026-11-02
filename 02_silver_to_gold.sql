-- Gold analytics layer: transaction-grain fact, dimensions and reusable KPIs.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.dim_customer` AS
SELECT CustomerID, CustomerSegment, KYCStatus, CreatedDate
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.customers_current`;

CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.dim_account` AS
SELECT AccountID, CustomerID, BranchID, AccountTypeID, SortCode, AccountNumber,
       OpenDate, CloseDate, Status, CurrentBalance, Currency
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current`;

CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions` AS
SELECT t.TransactionID, t.AccountID, a.CustomerID, t.TransactionTypeID,
       SAFE_CAST(t.TransactionDateTime AS TIMESTAMP) TransactionDateTime,
       DATE(SAFE_CAST(t.TransactionDateTime AS TIMESTAMP)) TransactionDate,
       SAFE_CAST(t.Amount AS NUMERIC) Amount, t.Currency, t.Channel,
       t.MerchantCategory, t.Description, SAFE_CAST(t.BalanceAfter AS NUMERIC) BalanceAfter,
       t.EmployeeID, tt.TypeName TransactionType, tt.Category TransactionCategory,
       t._batch_id, t._source_file, t._source_record_number
FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed` t
JOIN `project-437b3d91-7f41-4318-ad5.silver_dataset.accounts_current` a USING (AccountID)
LEFT JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.transaction_types` tt
  USING (TransactionTypeID);

-- 1) Top five customers by transaction count and value (separate rankings).
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_top5_customers` AS
WITH totals AS (
  SELECT CustomerID, COUNT(*) transaction_count,
         SUM(ABS(Amount)) gross_transaction_value
  FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions`
  GROUP BY CustomerID
)
SELECT *, RANK() OVER (ORDER BY transaction_count DESC) count_rank,
          RANK() OVER (ORDER BY gross_transaction_value DESC) value_rank
FROM totals
QUALIFY count_rank <= 5 OR value_rank <= 5;

-- 2) Transaction value by account product. Values retain source currency.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_product_transaction_value` AS
SELECT a.AccountTypeID, acct_type.TypeName AccountProduct, f.Currency,
       COUNT(*) transaction_count, SUM(ABS(f.Amount)) gross_transaction_value
FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions` f
JOIN `project-437b3d91-7f41-4318-ad5.gold_dataset.dim_account` a USING (AccountID)
LEFT JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.account_types` acct_type USING (AccountTypeID)
GROUP BY a.AccountTypeID, AccountProduct, f.Currency;

-- 3) Accounts with no transactions in the last 90 days from latest data date.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_inactive_accounts` AS
WITH bounds AS (
  SELECT DATE_SUB(MAX(TransactionDate), INTERVAL 90 DAY) cutoff
  FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions`
), last_activity AS (
  SELECT AccountID, MAX(TransactionDate) last_transaction_date
  FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions`
  GROUP BY AccountID
)
SELECT a.*, x.last_transaction_date, b.cutoff AS inactivity_cutoff
FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.dim_account` a
LEFT JOIN last_activity x USING (AccountID)
CROSS JOIN bounds b
WHERE UPPER(TRIM(a.Status)) = 'ACTIVE'
  AND (x.last_transaction_date IS NULL OR x.last_transaction_date < b.cutoff);

-- 4) High-value transactions: configurable threshold, GBP only for this rule.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_high_value_transactions` AS
SELECT * FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions`
WHERE UPPER(Currency) = 'GBP' AND ABS(Amount) > 100000;

-- 5) Loan repayment health and 6) card adoption.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_loan_repayment_health` AS
SELECT l.LoanID, l.CustomerID, l.BranchID, l.LoanType,
       SAFE_CAST(l.PrincipalAmount AS NUMERIC) PrincipalAmount,
       COUNT(p.PaymentID) installments,
       COUNTIF(UPPER(TRIM(p.PaymentStatus)) IN ('LATE','MISSED','OVERDUE')) late_or_missed,
       SUM(p.AmountDue) total_due, SUM(p.AmountPaid) total_paid,
       SAFE_DIVIDE(SUM(p.AmountPaid), SUM(p.AmountDue)) paid_ratio
FROM `project-437b3d91-7f41-4318-ad5.bronze_dataset.loans` l
LEFT JOIN `project-437b3d91-7f41-4318-ad5.silver_dataset.loan_payments_typed` p USING (LoanID)
GROUP BY l.LoanID, l.CustomerID, l.BranchID, l.LoanType, l.PrincipalAmount;

CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_card_adoption` AS
SELECT a.AccountTypeID, acct_type.TypeName AccountProduct, COUNT(DISTINCT a.AccountID) account_count,
       COUNT(DISTINCT IF(UPPER(TRIM(c.Status)) = 'ACTIVE', a.AccountID, NULL)) accounts_with_active_card,
       SAFE_DIVIDE(COUNT(DISTINCT IF(UPPER(TRIM(c.Status)) = 'ACTIVE', a.AccountID, NULL)),
                   COUNT(DISTINCT a.AccountID)) adoption_rate
FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.dim_account` a
LEFT JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.cards` c USING (AccountID)
LEFT JOIN `project-437b3d91-7f41-4318-ad5.bronze_dataset.account_types` acct_type USING (AccountTypeID)
GROUP BY a.AccountTypeID, AccountProduct;

-- 7) Explainable monitoring proxies; assumptions are documented in README.
-- RANGE frame gives each qualifying event the count in its trailing 24-hour window.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_structuring_proxy` AS
WITH candidates AS (
  SELECT AccountID, TransactionID, TransactionDateTime, ABS(Amount) amount_abs,
         Currency, Channel,
         COUNT(*) OVER (
           PARTITION BY AccountID
           ORDER BY UNIX_SECONDS(TransactionDateTime)
           RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
         ) AS qualifying_txns_24h,
         SUM(ABS(Amount)) OVER (
           PARTITION BY AccountID
           ORDER BY UNIX_SECONDS(TransactionDateTime)
           RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
         ) AS qualifying_value_24h
  FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions`
  WHERE UPPER(Currency) = 'GBP' AND UPPER(TransactionCategory) = 'CREDIT'
    AND UPPER(Channel) IN ('BRANCH','ATM') AND ABS(Amount) >= 8000 AND ABS(Amount) < 10000
)
SELECT * FROM candidates WHERE qualifying_txns_24h >= 3;

-- Dormancy proxy: after a >=90-day gap, at least 3 transactions within 7 days.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.kpi_dormant_reactivation_proxy` AS
WITH ordered AS (
  SELECT AccountID, TransactionID, TransactionDateTime,
         LAG(TransactionDateTime) OVER (PARTITION BY AccountID ORDER BY TransactionDateTime) prev_ts
  FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions`
), reactivated AS (
  SELECT a.AccountID, a.TransactionDateTime reactivation_time
  FROM ordered a
  WHERE prev_ts IS NOT NULL AND TIMESTAMP_DIFF(a.TransactionDateTime, prev_ts, DAY) >= 90
)
SELECT r.AccountID, r.reactivation_time, COUNT(*) transactions_in_7_days
FROM reactivated r
JOIN `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions` f
  ON f.AccountID = r.AccountID
 AND f.TransactionDateTime >= r.reactivation_time
 AND f.TransactionDateTime < TIMESTAMP_ADD(r.reactivation_time, INTERVAL 7 DAY)
GROUP BY r.AccountID, r.reactivation_time
HAVING COUNT(*) >= 3;

-- Blocking Gold integrity test. SCD2 dimensions must have exactly one current row/key.
CREATE OR REPLACE TABLE `project-437b3d91-7f41-4318-ad5.gold_dataset.pipeline_quality_summary` AS
SELECT 'fact_transaction_count_matches_silver' check_name,
       ABS((SELECT COUNT(*) FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.fact_transactions`) -
           (SELECT COUNT(*) FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.transactions_typed`)) failed_rows
UNION ALL
SELECT 'account_scd2_one_current', COUNTIF(n != 1)
FROM (SELECT AccountID, COUNTIF(is_current) n
      FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.dim_account_scd2` GROUP BY AccountID)
UNION ALL
SELECT 'customer_scd2_one_current', COUNTIF(n != 1)
FROM (SELECT CustomerID, COUNTIF(is_current) n
      FROM `project-437b3d91-7f41-4318-ad5.silver_dataset.dim_customer_scd2` GROUP BY CustomerID);

ASSERT (SELECT COUNTIF(failed_rows > 0) FROM `project-437b3d91-7f41-4318-ad5.gold_dataset.pipeline_quality_summary`) = 0
  AS 'Gold pipeline checks failed; inspect gold_dataset.pipeline_quality_summary';
