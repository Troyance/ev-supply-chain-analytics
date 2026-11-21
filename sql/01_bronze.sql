-- bronze: raw copies of the tpch sample tables, plus when/where we loaded them from

CREATE SCHEMA IF NOT EXISTS workspace.ev_supply_chain
  COMMENT 'Supply chain analytics side project';

CREATE OR REPLACE TABLE workspace.ev_supply_chain.bronze_supplier AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.supplier' AS _source
FROM samples.tpch.supplier;

CREATE OR REPLACE TABLE workspace.ev_supply_chain.bronze_nation AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.nation' AS _source
FROM samples.tpch.nation;

CREATE OR REPLACE TABLE workspace.ev_supply_chain.bronze_region AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.region' AS _source
FROM samples.tpch.region;

CREATE OR REPLACE TABLE workspace.ev_supply_chain.bronze_part AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.part' AS _source
FROM samples.tpch.part;

CREATE OR REPLACE TABLE workspace.ev_supply_chain.bronze_partsupp AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.partsupp' AS _source
FROM samples.tpch.partsupp;

CREATE OR REPLACE TABLE workspace.ev_supply_chain.bronze_orders AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.orders' AS _source
FROM samples.tpch.orders;

CREATE OR REPLACE TABLE workspace.ev_supply_chain.bronze_lineitem AS
SELECT *, current_timestamp() AS _ingested_at, 'samples.tpch.lineitem' AS _source
FROM samples.tpch.lineitem;
