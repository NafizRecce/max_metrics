-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ###Online Order Acceptance Record Table Load
-- MAGIC
-- MAGIC ####Description
-- MAGIC Max Order Acceptance model for Supervisoning Metrics Dashboard 
-- MAGIC ####Change History
-- MAGIC | Item   | Description | Version |  Modifier  |         Reason         |
-- MAGIC | ------ |------------ |---------| -----------| -----------------------|
-- MAGIC |#1.1    | 16/06/2024  |1.0      |   NI   |  Initial Version |
-- MAGIC

-- COMMAND ----------

SET spark.sql.session.timeZone = Australia/Melbourne

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####1. Set up Widgets and Helper Functions

-- COMMAND ----------

-- MAGIC %run /.libraries/shared-utils/2024.2.7/src/ddl_functions

-- COMMAND ----------

-- MAGIC %run ../../utils/helper_functions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.dropdown("catalog", "bidwh_legacy_sandbox", ["bidwh_legacy_sandbox", "bidwh_legacy_uat", "bidwh_legacy"])
-- MAGIC dbutils.widgets.dropdown("trs_catalog", "bidwh_legacy", ["bidwh_legacy_sandbox", "bidwh_legacy_uat", "bidwh_legacy"])
-- MAGIC dbutils.widgets.text("dimension_schema", "dim")
-- MAGIC dbutils.widgets.text("mds_schema", "mds")
-- MAGIC dbutils.widgets.text("staging_schema", "stg")
-- MAGIC dbutils.widgets.text("fact_schema", "fact")
-- MAGIC dbutils.widgets.text("subset_schema", "sbst")
-- MAGIC dbutils.widgets.text("job_run_id", "-1")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC trs_catalog = dbutils.widgets.get('trs_catalog')
-- MAGIC catalog = dbutils.widgets.get('catalog')
-- MAGIC mds_schema = dbutils.widgets.get('mds_schema')
-- MAGIC subset_schema = dbutils.widgets.get('subset_schema')
-- MAGIC staging_schema = dbutils.widgets.get('staging_schema')
-- MAGIC fact_schema = dbutils.widgets.get('fact_schema')
-- MAGIC job_run_id = int(dbutils.widgets.get('job_run_id'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = dbutils.widgets.get('catalog')
-- MAGIC dimension_schema = dbutils.widgets.get('dimension_schema')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####2. Create Temporary Metrics Schema Stg Views

-- COMMAND ----------

USE CATALOG ${catalog};
CREATE SCHEMA IF NOT EXISTS ${staging_schema};

-- COMMAND ----------

CREATE TEMPORARY VIEW orders AS
SELECT
  DISTINCT po.preorder_id,
  po.creation_date,
  po.customer_number,
  po.delivery_type,
  po.internal_order_type,
  required_by_date,
  d.dddatedelivered,
  d.ddtimedelivered,
  d.dddatereq,
  d.ddtimereq,
  CASE
    WHEN ghorddmy IS NULL THEN ihorddmy
    ELSE ghorddmy
  END AS date_processed,
  CASE
    WHEN ghordtime IS NULL THEN ihordtime
    ELSE ghordtime
  END AS time_processed
FROM
  ${catalog}.${fact_schema}.sales s
  INNER JOIN ${trs_catalog}.TRSAU.preorder po ON s.originaldocrefno = po.preorder_id
  AND s.SourceSystemSKey = 1
  INNER JOIN ${catalog}.${dimension_schema}.customer c ON po.customer_number = c.CustomerNumber
  AND s.CustomerSkey = c.CustomerSkey
  LEFT JOIN ${trs_catalog}.TRSAU.ghheads co ON po.internal_order_type = 'G'
  AND co.ghintref = po.internal_order_number
  AND po.customer_number = co.ghcustno
  LEFT JOIN ${trs_catalog}.TRSAU.invheads i ON i.ihintref = s.InvoiceHeaderNumber
  AND i.ihtype = 'I'
  LEFT JOIN ${trs_catalog}.TRSAU.deldetails d ON d.ddintref = ihintref
  AND d.ddtype = 'O'
WHERE
  (
    (
      po.creation_date BETWEEN '2023-01-01'
      AND '2023-03-30'
      AND po.creation_date NOT IN ('2023-01-01', '2023-01-26', '2023-03-13')
    )
    OR (
      po.creation_date BETWEEN '2022-01-01'
      AND '2022-03-30'
      AND po.creation_date NOT IN ('2022-01-03', '2022-01-26', '2022-03-14')
    )
    OR (
      po.creation_date BETWEEN '2023-07-01'
      AND '2023-09-30'
      AND po.creation_date NOT IN ('2023-08-07', '2023-09-25', '2023-09-29')
    )
    OR (
      po.creation_date BETWEEN '2023-09-01'
      AND '2023-12-31'
      AND po.creation_date NOT IN ('2023-12-25', '2023-12-26', '2023-11-07')
    )
  )
  AND dayofweek(po.creation_date) NOT IN (1, 7)
  AND po.order_status = 'P'
  AND po.customer_number <> 110;


-- COMMAND ----------

select * from orders;

-- COMMAND ----------



-- COMMAND ----------

USE CATALOG ${catalog};
CREATE SCHEMA IF NOT EXISTS ${fact_schema};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####3. Create and load Fact data to Fact table 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${catalog}.${fact_schema}.salescreditnote(
  ProcessExecutionID BIGINT NOT NULL,
  LoadDateTime TIMESTAMP,
  InvoiceHeaderNumber DECIMAL(10, 0),
  SalesCreditReasonCode INT,
  SalesCreditReasonDescription STRING,
  SalesCreditDisputedReasonCode INT,
  SalesCreditDisputedReasonDescription STRING,
  SourceSystemSkey INT,
  CONSTRAINT invoiceheadernumber_pk PRIMARY KEY (InvoiceHeaderNumber)
) COMMENT 'The "salescreditnote" table contains information about sales credit notes issued by the sales team. It includes details such as the invoice header number, sales credit reason codes, and descriptions. This data can be used to analyze sales credit note trends, understand the reasons behind credit disputes, and monitor the performance of the sales credit process. This information can be useful for finance teams to track and manage credit risk, and for operations teams to optimize the sales credit process.';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Add table tags to fact.sales
-- MAGIC alter_table_tag(catalog, fact_schema, "salescreditnote", tag_name="refresh_method", tag_value="Full Snapshot")
-- MAGIC alter_table_tag(catalog, fact_schema, "salescreditnote", tag_name="data_validation_status", tag_value="Validated")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####4. Insert sales data into Fact table

-- COMMAND ----------

INSERT
  OVERWRITE TABLE ${catalog}.${fact_schema}.salescreditnote
SELECT
  ${job_run_id} AS ProcessExecutionID,
  current_timestamp() AS LoadDateTime,
  *
FROM
  ${catalog}.${staging_schema}.salescreditnote;
