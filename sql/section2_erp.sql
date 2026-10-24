-- Section 2, Step 5: run in sqldb-erp -> Query editor, signed in with Microsoft Entra.
-- Replace the two passwords first (12+ chars, upper/lower/digit/symbol, no ; or ').

-- ERP tables (from the plan)
CREATE TABLE dbo.products  (item_id VARCHAR(30) PRIMARY KEY, dept_id VARCHAR(20), cat_id VARCHAR(20),
                            unit_price DECIMAL(8,2), supplier_id VARCHAR(10), updated_at DATETIME2);
CREATE TABLE dbo.stores    (store_id VARCHAR(10) PRIMARY KEY, state_id CHAR(2), city VARCHAR(40));
CREATE TABLE dbo.inventory (store_id VARCHAR(10), item_id VARCHAR(30), on_hand_qty INT,
                            reorder_point INT, updated_at DATETIME2,
                            CONSTRAINT pk_inv PRIMARY KEY (store_id, item_id));

-- Change data capture on inventory and products
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='inventory', @role_name=NULL, @supports_net_changes=1;
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='products',  @role_name=NULL, @supports_net_changes=1;
-- Keep change rows 30 days (default 3) so nothing is cleaned up before Section 3 reads it
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 43200;

-- Simulator: read and write the ERP tables
CREATE USER sim_writer WITH PASSWORD = '<sim password>';
ALTER ROLE db_datareader ADD MEMBER sim_writer;
ALTER ROLE db_datawriter ADD MEMBER sim_writer;

-- Databricks (Section 3): read tables and CDC functions only
CREATE USER dbx_reader WITH PASSWORD = '<reader password>';
GRANT SELECT ON SCHEMA::dbo TO dbx_reader;
GRANT SELECT ON SCHEMA::cdc TO dbx_reader;

-- Checks
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'sqldb-erp';
SELECT name, is_tracked_by_cdc FROM sys.tables;
SELECT capture_instance FROM cdc.change_tables;

-- After the simulator has run (Step 10)
-- SELECT COUNT(*) FROM cdc.dbo_inventory_CT;
-- SELECT COUNT(*) FROM cdc.dbo_products_CT;
