-- =============================================================================
-- Axon ETL Engine - Pedertractor
-- Schema: axon_pedertractor
--
-- Usage:
--   mysql -u <user> -p < schema.sql
--
-- This script is idempotent: safe to run multiple times.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS axon_pedertractor
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE axon_pedertractor;

-- -----------------------------------------------------------------------------
-- customers
-- One row per unique customer (identified by CNPJ).
-- Updated in-place whenever a release arrives with new customer data.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
  id            INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  cnpj          VARCHAR(14)     NOT NULL COMMENT 'Only digits, 14 chars',
  internal_code VARCHAR(50)     NOT NULL,
  company_name  VARCHAR(255)    DEFAULT NULL,
  trade_name    VARCHAR(255)    DEFAULT NULL,
  alias         VARCHAR(255)    DEFAULT NULL,
  municipality  VARCHAR(100)    DEFAULT NULL,
  state         VARCHAR(10)     DEFAULT NULL,
  country       VARCHAR(100)    DEFAULT NULL,
  created_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_customers_cnpj (cnpj)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- releases
-- One row per release document received via POST /releases.
-- custom_id is the business key (e.g. "c:1248|rd:20240424|r:256246").
-- release_status tracks the loading lifecycle: loading → loaded | load_failed.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS releases (
  id                  INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  custom_id           VARCHAR(255)    NOT NULL,
  customer_id         INT UNSIGNED    NOT NULL,
  customer_release_id VARCHAR(100)    NOT NULL,
  release_date        DATE            NOT NULL,
  file_name           VARCHAR(500)    DEFAULT NULL,
  receipt_file_name   VARCHAR(500)    DEFAULT NULL,
  arrival_timestamp   DATETIME(3)     DEFAULT NULL,
  items_qty           INT             DEFAULT 0,
  deliveries_qty      INT             DEFAULT 0,
  `force`             TINYINT(1)      NOT NULL DEFAULT 0,
  release_status      VARCHAR(20)     NOT NULL DEFAULT 'loading'
                        COMMENT 'loading | loaded | load_failed',
  created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_releases_custom_id (custom_id),
  KEY idx_releases_customer_id (customer_id),
  KEY idx_releases_release_date (release_date),

  CONSTRAINT fk_releases_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- release_items
-- One row per line item inside a release.
-- custom_id example: "c:1248|rd:20240424|r:256246|p:SA30609648|i:47658388"
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS release_items (
  id                              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  custom_id                       VARCHAR(500)    NOT NULL,
  release_id                      INT UNSIGNED    NOT NULL,
  sequence                        INT             DEFAULT NULL,
  customer_purchase_order         VARCHAR(100)    DEFAULT NULL,
  purchase_order_line             INT             DEFAULT NULL,
  program_id                      VARCHAR(100)    DEFAULT NULL,
  program_date                    DATE            DEFAULT NULL,
  program_type                    VARCHAR(50)     DEFAULT NULL,
  customer_pn                     VARCHAR(100)    DEFAULT NULL,
  technical_revision              VARCHAR(20)     DEFAULT NULL,
  supplier_pn                     VARCHAR(100)    DEFAULT NULL,
  unit_of_measure                 VARCHAR(20)     DEFAULT NULL,
  min_batch_qty                   DECIMAL(18,4)   DEFAULT NULL,
  last_received_date              DATE            DEFAULT NULL,
  last_received_qty               DECIMAL(18,4)   DEFAULT NULL,
  last_invoice_number             VARCHAR(100)    DEFAULT NULL,
  last_invoice_series             VARCHAR(20)     DEFAULT NULL,
  last_invoice_date               DATE            DEFAULT NULL,
  last_acc_qty                    DECIMAL(18,4)   DEFAULT NULL,
  last_acc_needed_qty             DECIMAL(18,4)   DEFAULT NULL,
  acc_start_date                  DATE            DEFAULT NULL,
  delivery_location               VARCHAR(255)    DEFAULT NULL,
  contact_person                  VARCHAR(255)    DEFAULT NULL,
  supply_type                     VARCHAR(100)    DEFAULT NULL,
  supply_frequency_code           VARCHAR(50)     DEFAULT NULL,
  production_authorization_date   DATE            DEFAULT NULL,
  raw_material_authorization_date DATE            DEFAULT NULL,
  unload_location                 VARCHAR(255)    DEFAULT NULL,
  item_status_code                VARCHAR(50)     DEFAULT NULL,
  notes                           TEXT            DEFAULT NULL,
  created_at                      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at                      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_release_items_custom_id (custom_id),
  KEY idx_release_items_release_id (release_id),
  KEY idx_release_items_customer_pn (customer_pn),

  CONSTRAINT fk_release_items_release
    FOREIGN KEY (release_id) REFERENCES releases (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- release_deliveries
-- One row per delivery schedule line inside a release item.
-- custom_id example: "c:1248|rd:20240424|r:256246|p:SA30609648|i:47658388|d:20240709|s:10"
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS release_deliveries (
  id                    INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  custom_id             VARCHAR(600)    NOT NULL,
  item_id               INT UNSIGNED    NOT NULL,
  sequence              INT             DEFAULT NULL,
  type                  VARCHAR(50)     DEFAULT NULL COMMENT 'firm | planning',
  due_date              DATE            DEFAULT NULL,
  delivery_time         TIME            DEFAULT NULL,
  qty                   DECIMAL(18,4)   DEFAULT NULL,
  delivery_window_start DATETIME        DEFAULT NULL,
  acc_qty               DECIMAL(18,4)   DEFAULT NULL,
  created_at            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_release_deliveries_custom_id (custom_id),
  KEY idx_release_deliveries_item_id (item_id),
  KEY idx_release_deliveries_due_date (due_date),

  CONSTRAINT fk_release_deliveries_item
    FOREIGN KEY (item_id) REFERENCES release_items (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- release_analyses
-- One row per analysis run triggered via POST /analysis.
-- release_id references the releases table (matched by customer_release_id).
-- analysis_configs stores the full AnalysisConfigs object as JSON.
-- analysis_status tracks the analysis lifecycle:
--   not_analyzed → analyzing → analyzed | analysis_failed
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS release_analyses (
  id                           INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  release_id                   INT UNSIGNED    NOT NULL,
  `force`                      TINYINT(1)      NOT NULL DEFAULT 0,
  analysis_version             INT             NOT NULL DEFAULT 1,

  -- AnalysisConfigs fields (flattened for easy querying)
  firm_policy                  VARCHAR(50)     DEFAULT NULL  COMMENT 'release | custom',
  custom_firm_days             INT             DEFAULT NULL,
  accept_increment             TINYINT(1)      DEFAULT NULL,
  accept_cut                   TINYINT(1)      DEFAULT NULL,
  accept_date_variation        TINYINT(1)      DEFAULT NULL,
  transit_qty_policy           VARCHAR(50)     DEFAULT NULL  COMMENT 'acc | ...',
  create_order_if_not_exists   TINYINT(1)      DEFAULT NULL,
  auto_implement_analysis_result TINYINT(1)    DEFAULT NULL,
  use_leadtime                 TINYINT(1)      DEFAULT NULL,
  default_leadtime             INT             DEFAULT NULL,
  use_receipt                  TINYINT(1)      DEFAULT NULL,

  -- Full configs snapshot for auditability
  analysis_configs_json        JSON            DEFAULT NULL,

  analysis_status              VARCHAR(20)     NOT NULL DEFAULT 'not_analyzed'
                                 COMMENT 'not_analyzed | analyzing | analyzed | analysis_failed',
  analysis_duration            VARCHAR(50)     DEFAULT NULL,

  -- AnalysisTotals (populated later via PUT /analysis/duration-and-totals)
  totals_json                  JSON            DEFAULT NULL,

  created_at                   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at                   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_release_analyses_release_id (release_id),
  KEY idx_release_analyses_status (analysis_status),

  CONSTRAINT fk_release_analyses_release
    FOREIGN KEY (release_id) REFERENCES releases (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- release_analysis_items
-- One row per item analysis result inside a release_analyses record.
-- custom_id example: "c:1248|rd:20240424|r:256246|v:1|p:SA30630744|i:47424907"
-- comments stored as JSON array of strings.
-- implementation_comments stored as JSON array of strings.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS release_analysis_items (
  id                              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  custom_id                       VARCHAR(600)    NOT NULL,
  release_analysis_id             INT UNSIGNED    NOT NULL,
  sequence                        INT             DEFAULT NULL,
  customer_purchase_order         VARCHAR(100)    DEFAULT NULL,
  customer_pn                     VARCHAR(100)    DEFAULT NULL,
  customer_technical_revision     VARCHAR(20)     DEFAULT NULL,
  supplier_pn                     VARCHAR(100)    DEFAULT NULL,
  supplier_technical_revision     VARCHAR(20)     DEFAULT NULL,
  customer_acc_qty                DECIMAL(18,4)   DEFAULT NULL,
  supplier_acc_qty                DECIMAL(18,4)   DEFAULT NULL,
  transit_acc_qty                 DECIMAL(18,4)   DEFAULT NULL,
  customer_last_invoice_number    VARCHAR(100)    DEFAULT NULL,
  supplier_last_invoice_number    VARCHAR(100)    DEFAULT NULL,
  transit_invoice_qty             DECIMAL(18,4)   DEFAULT NULL,
  backlog_firm_date               DATE            DEFAULT NULL,
  release_firm_date               DATE            DEFAULT NULL,
  backlog_firm_qty                DECIMAL(18,4)   DEFAULT NULL,
  release_firm_qty                DECIMAL(18,4)   DEFAULT NULL,
  release_previous_firm_qty       DECIMAL(18,4)   DEFAULT NULL,
  firm_qty_variation              DECIMAL(18,4)   DEFAULT NULL,
  variation_type                  VARCHAR(50)     DEFAULT NULL,
  is_missing                      TINYINT(1)      DEFAULT NULL,
  is_uncorrelated                 TINYINT(1)      DEFAULT NULL,
  has_qty_less_than_min_order_qty         TINYINT(1) DEFAULT NULL,
  has_qty_not_multiple_of_min_order_qty   TINYINT(1) DEFAULT NULL,
  qty_left_add                    DECIMAL(18,4)   DEFAULT NULL,
  excess_cut_qty                  DECIMAL(18,4)   DEFAULT NULL,
  backlog_total_qty               DECIMAL(18,4)   DEFAULT NULL,
  release_total_qty               DECIMAL(18,4)   DEFAULT NULL,
  total_qty_variation             DECIMAL(18,4)   DEFAULT NULL,
  analysis_result_firm_date       DATE            DEFAULT NULL,
  analysis_result_firm_qty        DECIMAL(18,4)   DEFAULT NULL,
  analysis_result_total_qty       DECIMAL(18,4)   DEFAULT NULL,
  comments                        JSON            DEFAULT NULL,
  is_implemented                  TINYINT(1)      DEFAULT 0,
  implementation_comments         JSON            DEFAULT NULL,
  created_at                      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at                      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_release_analysis_items_custom_id (custom_id),
  KEY idx_release_analysis_items_analysis_id (release_analysis_id),
  KEY idx_release_analysis_items_customer_pn (customer_pn),

  CONSTRAINT fk_release_analysis_items_analysis
    FOREIGN KEY (release_analysis_id) REFERENCES release_analyses (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- release_analysis_deliveries
-- One row per delivery line inside a release_analysis_items record.
-- custom_id example: "c:1248|rd:20240424|r:256246|v:1|p:SA30630744|i:47424907|d:20240502|s:10"
-- comments stored as JSON array of strings.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS release_analysis_deliveries (
  id                              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  custom_id                       VARCHAR(700)    NOT NULL,
  analysis_item_id                INT UNSIGNED    NOT NULL,
  sequence                        INT             DEFAULT NULL,
  due_date                        DATE            DEFAULT NULL,
  delivery_time                   TIME            DEFAULT NULL,
  backlog_delivery_type           VARCHAR(50)     DEFAULT NULL,
  backlog_qty                     DECIMAL(18,4)   DEFAULT NULL,
  backlog_acc_qty                 DECIMAL(18,4)   DEFAULT NULL,
  release_delivery_type           VARCHAR(50)     DEFAULT NULL,
  release_qty                     DECIMAL(18,4)   DEFAULT NULL,
  release_acc_qty                 DECIMAL(18,4)   DEFAULT NULL,
  qty_variation                   DECIMAL(18,4)   DEFAULT NULL,
  acc_qty_variation               DECIMAL(18,4)   DEFAULT NULL,
  analysis_result_delivery_type   VARCHAR(50)     DEFAULT NULL,
  analysis_result_qty             DECIMAL(18,4)   DEFAULT NULL,
  analysis_result_acc_qty         DECIMAL(18,4)   DEFAULT NULL,
  analysis_result_qty_variation   DECIMAL(18,4)   DEFAULT NULL,
  analysis_result_acc_qty_variation DECIMAL(18,4) DEFAULT NULL,
  comments                        JSON            DEFAULT NULL,
  created_at                      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at                      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_release_analysis_deliveries_custom_id (custom_id),
  KEY idx_release_analysis_deliveries_item_id (analysis_item_id),
  KEY idx_release_analysis_deliveries_due_date (due_date),

  CONSTRAINT fk_release_analysis_deliveries_item
    FOREIGN KEY (analysis_item_id) REFERENCES release_analysis_items (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- orders
-- One row per customer purchase order created via POST /orders.
-- customer_code is the internalCode of the customer (not the CNPJ).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
  id                      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  customer_code           VARCHAR(50)     NOT NULL,
  customer_purchase_order VARCHAR(100)    NOT NULL,
  created_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uq_orders_customer_po (customer_code, customer_purchase_order),
  KEY idx_orders_customer_code (customer_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- order_items
-- One row per item inside an order.
-- analysis_result_json stores the AnalysisResult snapshot sent at insertion time.
-- When an item is inserted, the corresponding release_analysis_items row should
-- have is_implemented set to true (done in application layer).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
  id                          INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  order_id                    INT UNSIGNED    NOT NULL,
  customer_pn                 VARCHAR(100)    DEFAULT NULL,
  customer_technical_revision VARCHAR(20)     DEFAULT NULL,
  supplier_pn                 VARCHAR(100)    DEFAULT NULL,
  supplier_technical_revision VARCHAR(20)     DEFAULT NULL,
  lifecycle_stage             VARCHAR(50)     DEFAULT NULL,
  category                    VARCHAR(100)    DEFAULT NULL,
  lead_time                   INT             DEFAULT NULL,
  item_analysis_id            INT UNSIGNED    DEFAULT NULL
                                COMMENT 'FK to release_analysis_items.id (nullable)',
  analysis_result_json        JSON            DEFAULT NULL,
  created_at                  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at                  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_order_items_order_id (order_id),
  KEY idx_order_items_customer_pn (customer_pn),
  KEY idx_order_items_item_analysis_id (item_analysis_id),

  CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES orders (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- order_deliveries
-- One row per delivery schedule line inside an order item.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_deliveries (
  id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  order_item_id   INT UNSIGNED    NOT NULL,
  sequence        INT             DEFAULT NULL,
  type            VARCHAR(50)     DEFAULT NULL COMMENT 'firm | planning',
  due_date        DATE            DEFAULT NULL,
  delivery_time   TIME            DEFAULT NULL,
  qty             DECIMAL(18,4)   DEFAULT NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_order_deliveries_order_item_id (order_item_id),
  KEY idx_order_deliveries_due_date (due_date),

  CONSTRAINT fk_order_deliveries_item
    FOREIGN KEY (order_item_id) REFERENCES order_items (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
