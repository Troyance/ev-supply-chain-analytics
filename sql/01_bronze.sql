-- bronze: land the tpch tables the pipeline actually uses (supplier master and the part id pool) in ev_bronze,
-- with when / where we loaded them from. in a real setup this is the fivetran sync.

CREATE SCHEMA IF NOT EXISTS workspace.ev_bronze
  COMMENT 'Bronze: source system extracts (erp, mes, wms, service) plus ml and simulation outputs. Written by the spark notebooks, read by dbt sources.';

CREATE SCHEMA IF NOT EXISTS workspace.ev_ml
  COMMENT 'Registered forecasting models.';

CREATE OR REPLACE TABLE workspace.ev_bronze.tpch_supplier AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.supplier' AS _source FROM samples.tpch.supplier;

CREATE OR REPLACE TABLE workspace.ev_bronze.tpch_nation AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.nation' AS _source FROM samples.tpch.nation;

CREATE OR REPLACE TABLE workspace.ev_bronze.tpch_region AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.region' AS _source FROM samples.tpch.region;

CREATE OR REPLACE TABLE workspace.ev_bronze.tpch_part AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.part' AS _source FROM samples.tpch.part;
