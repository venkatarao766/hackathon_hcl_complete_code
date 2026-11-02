-- Step 1: run once in BigQuery Standard SQL.
-- Choose one location for all three datasets and use that same value below.
-- Change US if your bucket/Dataproc/BigQuery datasets use another location.
CREATE SCHEMA IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset` OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.silver_dataset` OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.gold_dataset` OPTIONS(location="US");

-- Bronze deliberately preserves every source business field as STRING.
-- The five trailing columns are ingestion lineage and replay identity.
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.branches` (
 BranchID STRING, BranchName STRING, Town STRING, County STRING, Region STRING, SortCodePrefix STRING, OpenDate STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.employees` (
 EmployeeID STRING, BranchID STRING, FirstName STRING, LastName STRING, JobTitle STRING, HireDate STRING, ManagerID STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.customers` (
 CustomerID STRING, FirstName STRING, LastName STRING, DateOfBirth STRING, NationalInsuranceNumber STRING, Email STRING, Phone STRING, AddressLine1 STRING, Town STRING, County STRING, PostCode STRING, CustomerSegment STRING, KYCStatus STRING, CreatedDate STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.account_types` (
 AccountTypeID STRING, TypeName STRING, InterestRate STRING, MinBalance STRING, OverdraftLimit STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.accounts` (
 AccountID STRING, CustomerID STRING, BranchID STRING, AccountTypeID STRING, SortCode STRING, AccountNumber STRING, OpenDate STRING, CloseDate STRING, Status STRING, CurrentBalance STRING, Currency STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.cards` (
 CardID STRING, AccountID STRING, CardNumberMasked STRING, CardType STRING, IssueDate STRING, ExpiryDate STRING, Status STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.transaction_types` (
 TransactionTypeID STRING, TypeName STRING, Category STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.transactions` (
 TransactionID STRING, AccountID STRING, TransactionTypeID STRING, TransactionDateTime STRING, Amount STRING, Currency STRING, Channel STRING, MerchantCategory STRING, Description STRING, BalanceAfter STRING, EmployeeID STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.loans` (
 LoanID STRING, CustomerID STRING, BranchID STRING, LoanType STRING, PrincipalAmount STRING, InterestRate STRING, TermMonths STRING, StartDate STRING, Status STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.loan_payments` (
 PaymentID STRING, LoanID STRING, DueDate STRING, PaymentDate STRING, AmountDue STRING, AmountPaid STRING, PaymentStatus STRING,
 _source_file STRING, _batch_id STRING, _source_record_number INT64, _ingested_at TIMESTAMP, _record_hash STRING);

-- Metadata tables make entity-to-table mapping visible and editable.
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.metadata_entity_config` (
 entity_name STRING, target_table STRING, business_columns ARRAY<STRING>, primary_key STRING,
 required_columns ARRAY<STRING>, expected_batches ARRAY<STRING>, load_behavior STRING);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.metadata_validation_rules` (
 rule_id STRING, entity_name STRING, severity STRING, action STRING, rule_description STRING, active BOOL);
CREATE TABLE IF NOT EXISTS `project-437b3d91-7f41-4318-ad5.bronze_dataset.metadata_assumptions` (
 assumption_id STRING, assumption STRING, rationale STRING, status STRING);

-- Entity rows are loaded from metadata_entity_config.jsonl by Composer before PySpark runs.

MERGE `project-437b3d91-7f41-4318-ad5.bronze_dataset.metadata_validation_rules` T
USING UNNEST([
 STRUCT('CUSTOMER_EMAIL_MISSING' AS rule_id,'CUSTOMERS' AS entity_name,'WARNING' AS severity,'QUARANTINE' AS action,'Current customer email is blank or null.' AS rule_description,TRUE AS active),
 STRUCT('CUSTOMER_PHONE_MISSING','CUSTOMERS','WARNING','QUARANTINE','Current customer phone is blank or null.',TRUE),
 STRUCT('TRANSACTION_CURRENCY_MISMATCH','TRANSACTIONS','WARNING','QUARANTINE','Transaction currency differs from the linked current account currency; review FX validity.',TRUE),
 STRUCT('ACCOUNT_SORT_CODE_PREFIX_MISMATCH','ACCOUNTS','WARNING','QUARANTINE','Sort code does not start with the linked branch prefix.',TRUE),
 STRUCT('ACCOUNT_OVERDRAFT_LIMIT_EXCEEDED','ACCOUNTS','WARNING','QUARANTINE','Current balance is below the negative configured overdraft limit.',TRUE),
 STRUCT('DUPLICATE_TRANSACTION_ID','TRANSACTIONS','ERROR','BLOCK','Transaction ID appears more than once.',TRUE),
 STRUCT('ACCOUNT_CUSTOMER_ORPHAN','ACCOUNTS','ERROR','BLOCK','Account customer key must exist in current customers.',TRUE),
 STRUCT('ACCOUNT_BRANCH_ORPHAN','ACCOUNTS','ERROR','BLOCK','Account branch key must exist in branches.',TRUE),
 STRUCT('ACCOUNT_TYPE_ORPHAN','ACCOUNTS','ERROR','BLOCK','Account type key must exist in account types.',TRUE),
 STRUCT('TRANSACTION_ACCOUNT_ORPHAN','TRANSACTIONS','ERROR','BLOCK','Transaction account key must exist in current accounts.',TRUE),
 STRUCT('TRANSACTION_TYPE_ORPHAN','TRANSACTIONS','ERROR','BLOCK','Transaction type key must exist in transaction types.',TRUE),
 STRUCT('TRANSACTION_AMOUNT_INVALID','TRANSACTIONS','ERROR','BLOCK','Transaction amount must parse as NUMERIC.',TRUE),
 STRUCT('TRANSACTION_DATETIME_INVALID','TRANSACTIONS','ERROR','BLOCK','Transaction datetime must parse as TIMESTAMP.',TRUE)
]) S ON T.rule_id=S.rule_id
WHEN MATCHED THEN UPDATE SET entity_name=S.entity_name,severity=S.severity,action=S.action,rule_description=S.rule_description,active=S.active
WHEN NOT MATCHED THEN INSERT(rule_id,entity_name,severity,action,rule_description,active)
VALUES(S.rule_id,S.entity_name,S.severity,S.action,S.rule_description,S.active);

-- Insert the documented demo assumptions once; safe to rerun.
MERGE `project-437b3d91-7f41-4318-ad5.bronze_dataset.metadata_assumptions` T
USING UNNEST([
 STRUCT('SCD2_EFFECTIVE_DATES' AS assumption_id,'day0/day1/day2 map to 2026-10-26/27/28' AS assumption,'Replace with source business-effective dates if supplied.' AS rationale,'DEMO' AS status),
 STRUCT('FX_TREATMENT','Amounts retain source currency','Never aggregate across currencies without approved FX rates and conversion policy.','REVIEW'),
 STRUCT('HIGH_VALUE_THRESHOLD','GBP absolute amount > 100000','Illustrative use-case threshold; obtain business approval.','DEMO'),
 STRUCT('STRUCTURING_PROXY','Credit proxy, Branch/ATM, GBP 8000 to <10000; 3+ in 24 hours','Monitoring proxy only; not a suspicious activity determination.','DEMO'),
 STRUCT('DORMANCY_PROXY','90-day gap followed by 3+ transactions in 7 days','Sample history may be too short to produce findings.','DEMO'),
 STRUCT('INACTIVE_ACCOUNT','Active account with no transaction in 90 days before latest transaction date','Activity heuristic based on this sample.','DEMO'),
 STRUCT('OVERDRAFT_RULE','Balance below negative configured overdraft limit','Review source product policy, especially zero-limit products.','REVIEW'),
 STRUCT('SORT_CODE_RULE','Account sort code begins with linked branch prefix','Data quality check; preserve and quarantine source values.','REVIEW')
]) S ON T.assumption_id=S.assumption_id
WHEN NOT MATCHED THEN INSERT(assumption_id,assumption,rationale,status)
VALUES(S.assumption_id,S.assumption,S.rationale,S.status);
