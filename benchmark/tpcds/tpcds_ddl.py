from .tpcds_table_names import parametrize_tpcds_statements

_TPCDS_DDL_RAW = [
    (
        "call_center",
        """
        -- Snowflake-like DDL for TPC-DS CALL_CENTER
        CREATE OR REPLACE TABLE {CALL_CENTER_TABLE} (
          cc_call_center_sk INTEGER,
          cc_call_center_id VARCHAR,
          cc_rec_start_date DATE,
          cc_rec_end_date DATE,
          cc_closed_date_sk INTEGER,
          cc_open_date_sk INTEGER,
          cc_name VARCHAR,
          cc_class VARCHAR,
          cc_employees INTEGER,
          cc_sq_ft INTEGER,
          cc_hours VARCHAR,
          cc_manager VARCHAR,
          cc_mkt_id INTEGER,
          cc_mkt_class VARCHAR,
          cc_mkt_desc VARCHAR,
          cc_market_manager VARCHAR,
          cc_division INTEGER,
          cc_division_name VARCHAR,
          cc_company INTEGER,
          cc_company_name VARCHAR,
          cc_street_number VARCHAR,
          cc_street_name VARCHAR,
          cc_street_type VARCHAR,
          cc_suite_number VARCHAR,
          cc_city VARCHAR,
          cc_county VARCHAR,
          cc_state VARCHAR,
          cc_zip VARCHAR,
          cc_country VARCHAR,
          cc_gmt_offset DECIMAL(5,
          2),
          cc_tax_percentage DECIMAL(5,
          2)
        );
        """
    ),

    (
        "catalog_page",
        """
        -- Snowflake-like DDL for TPC-DS CATALOG_PAGE
        CREATE OR REPLACE TABLE {CATALOG_PAGE_TABLE} (
          cp_catalog_page_sk INTEGER,
          cp_catalog_page_id VARCHAR,
          cp_start_date_sk INTEGER,
          cp_end_date_sk INTEGER,
          cp_department VARCHAR,
          cp_catalog_number INTEGER,
          cp_catalog_page_number INTEGER,
          cp_description VARCHAR,
          cp_type VARCHAR
        );
        """
    ),

    (
        "catalog_returns",
        """
        -- Snowflake-like DDL for TPC-DS CATALOG_RETURNS
        CREATE OR REPLACE TABLE {CATALOG_RETURNS_TABLE} (
          cr_returned_date_sk INTEGER,
          cr_returned_time_sk INTEGER,
          cr_item_sk INTEGER,
          cr_refunded_customer_sk INTEGER,
          cr_refunded_cdemo_sk INTEGER,
          cr_refunded_hdemo_sk INTEGER,
          cr_refunded_addr_sk INTEGER,
          cr_returning_customer_sk INTEGER,
          cr_returning_cdemo_sk INTEGER,
          cr_returning_hdemo_sk INTEGER,
          cr_returning_addr_sk INTEGER,
          cr_call_center_sk INTEGER,
          cr_catalog_page_sk INTEGER,
          cr_ship_mode_sk INTEGER,
          cr_warehouse_sk INTEGER,
          cr_reason_sk INTEGER,
          cr_order_number INTEGER,
          cr_return_quantity INTEGER,
          cr_return_amount DECIMAL(7,
          2),
          cr_return_tax DECIMAL(7,
          2),
          cr_return_amt_inc_tax DECIMAL(7,
          2),
          cr_fee DECIMAL(7,
          2),
          cr_return_ship_cost DECIMAL(7,
          2),
          cr_refunded_cash DECIMAL(7,
          2),
          cr_reversed_charge DECIMAL(7,
          2),
          cr_store_credit DECIMAL(7,
          2),
          cr_net_loss DECIMAL(7,
          2)
        );
        """
    ),

    (
        "catalog_sales",
        """
        -- Snowflake-like DDL for TPC-DS CATALOG_SALES
        CREATE OR REPLACE TABLE {CATALOG_SALES_TABLE} (
          cs_sold_date_sk INTEGER,
          cs_sold_time_sk INTEGER,
          cs_ship_date_sk INTEGER,
          cs_bill_customer_sk INTEGER,
          cs_bill_cdemo_sk INTEGER,
          cs_bill_hdemo_sk INTEGER,
          cs_bill_addr_sk INTEGER,
          cs_ship_customer_sk INTEGER,
          cs_ship_cdemo_sk INTEGER,
          cs_ship_hdemo_sk INTEGER,
          cs_ship_addr_sk INTEGER,
          cs_call_center_sk INTEGER,
          cs_catalog_page_sk INTEGER,
          cs_ship_mode_sk INTEGER,
          cs_warehouse_sk INTEGER,
          cs_item_sk INTEGER,
          cs_promo_sk INTEGER,
          cs_order_number INTEGER,
          cs_quantity INTEGER,
          cs_wholesale_cost DECIMAL(7,
          2),
          cs_list_price DECIMAL(7,
          2),
          cs_sales_price DECIMAL(7,
          2),
          cs_ext_discount_amt DECIMAL(7,
          2),
          cs_ext_sales_price DECIMAL(7,
          2),
          cs_ext_wholesale_cost DECIMAL(7,
          2),
          cs_ext_list_price DECIMAL(7,
          2),
          cs_ext_tax DECIMAL(7,
          2),
          cs_coupon_amt DECIMAL(7,
          2),
          cs_ext_ship_cost DECIMAL(7,
          2),
          cs_net_paid DECIMAL(7,
          2),
          cs_net_paid_inc_tax DECIMAL(7,
          2),
          cs_net_paid_inc_ship DECIMAL(7,
          2),
          cs_net_paid_inc_ship_tax DECIMAL(7,
          2),
          cs_net_profit DECIMAL(7,
          2)
        );
        """
    ),

    (
        "customer",
        """
        -- Snowflake-like DDL for TPC-DS CUSTOMER
        CREATE OR REPLACE TABLE {CUSTOMER_TABLE} (
          c_customer_sk INTEGER,
          c_customer_id VARCHAR,
          c_current_cdemo_sk INTEGER,
          c_current_hdemo_sk INTEGER,
          c_current_addr_sk INTEGER,
          c_first_shipto_date_sk INTEGER,
          c_first_sales_date_sk INTEGER,
          c_salutation VARCHAR,
          c_first_name VARCHAR,
          c_last_name VARCHAR,
          c_preferred_cust_flag VARCHAR,
          c_birth_day INTEGER,
          c_birth_month INTEGER,
          c_birth_year INTEGER,
          c_birth_country VARCHAR,
          c_login VARCHAR,
          c_email_address VARCHAR,
          c_last_review_date_sk INTEGER
        );
        """
    ),

    (
        "customer_address",
        """
        -- Snowflake-like DDL for TPC-DS CUSTOMER_ADDRESS
        CREATE OR REPLACE TABLE {CUSTOMER_ADDRESS_TABLE} (
          ca_address_sk INTEGER,
          ca_address_id VARCHAR,
          ca_street_number VARCHAR,
          ca_street_name VARCHAR,
          ca_street_type VARCHAR,
          ca_suite_number VARCHAR,
          ca_city VARCHAR,
          ca_county VARCHAR,
          ca_state VARCHAR,
          ca_zip VARCHAR,
          ca_country VARCHAR,
          ca_gmt_offset DECIMAL(5,
          2),
          ca_location_type VARCHAR
        );
        """
    ),

    (
        "customer_demographics",
        """
        -- Snowflake-like DDL for TPC-DS CUSTOMER_DEMOGRAPHICS
        CREATE OR REPLACE TABLE {CUSTOMER_DEMOGRAPHICS_TABLE} (
          cd_demo_sk INTEGER,
          cd_gender VARCHAR,
          cd_marital_status VARCHAR,
          cd_education_status VARCHAR,
          cd_purchase_estimate INTEGER,
          cd_credit_rating VARCHAR,
          cd_dep_count INTEGER,
          cd_dep_employed_count INTEGER,
          cd_dep_college_count INTEGER
        );
        """
    ),

    (
        "date_dim",
        """
        -- Snowflake-like DDL for TPC-DS DATE_DIM
        CREATE OR REPLACE TABLE {DATE_DIM_TABLE} (
          d_date_sk INTEGER,
          d_date_id VARCHAR,
          d_date DATE,
          d_month_seq INTEGER,
          d_week_seq INTEGER,
          d_quarter_seq INTEGER,
          d_year INTEGER,
          d_dow INTEGER,
          d_moy INTEGER,
          d_dom INTEGER,
          d_qoy INTEGER,
          d_fy_year INTEGER,
          d_fy_quarter_seq INTEGER,
          d_fy_week_seq INTEGER,
          d_day_name VARCHAR,
          d_quarter_name VARCHAR,
          d_holiday VARCHAR,
          d_weekend VARCHAR,
          d_following_holiday VARCHAR,
          d_first_dom INTEGER,
          d_last_dom INTEGER,
          d_same_day_ly INTEGER,
          d_same_day_lq INTEGER,
          d_current_day VARCHAR,
          d_current_week VARCHAR,
          d_current_month VARCHAR,
          d_current_quarter VARCHAR,
          d_current_year VARCHAR
        );
        """
    ),

    (
        "household_demographics",
        """
        -- Snowflake-like DDL for TPC-DS HOUSEHOLD_DEMOGRAPHICS
        CREATE OR REPLACE TABLE {HOUSEHOLD_DEMOGRAPHICS_TABLE} (
          hd_demo_sk INTEGER,
          hd_income_band_sk INTEGER,
          hd_buy_potential VARCHAR,
          hd_dep_count INTEGER,
          hd_vehicle_count INTEGER
        );
        """
    ),

    (
        "income_band",
        """
        -- Snowflake-like DDL for TPC-DS INCOME_BAND
        CREATE OR REPLACE TABLE {INCOME_BAND_TABLE} (
          ib_income_band_sk INTEGER,
          ib_lower_bound INTEGER,
          ib_upper_bound INTEGER
        );
        """
    ),

    (
        "inventory",
        """
        -- Snowflake-like DDL for TPC-DS INVENTORY
        CREATE OR REPLACE TABLE {INVENTORY_TABLE} (
          inv_date_sk INTEGER,
          inv_item_sk INTEGER,
          inv_warehouse_sk INTEGER,
          inv_quantity_on_hand INTEGER
        );
        """
    ),

    (
        "item",
        """
        -- Snowflake-like DDL for TPC-DS ITEM
        CREATE OR REPLACE TABLE {ITEM_TABLE} (
          i_item_sk INTEGER,
          i_item_id VARCHAR,
          i_rec_start_date DATE,
          i_rec_end_date DATE,
          i_item_desc VARCHAR,
          i_current_price DECIMAL(7,
          2),
          i_wholesale_cost DECIMAL(7,
          2),
          i_brand_id INTEGER,
          i_brand VARCHAR,
          i_class_id INTEGER,
          i_class VARCHAR,
          i_category_id INTEGER,
          i_category VARCHAR,
          i_manufact_id INTEGER,
          i_manufact VARCHAR,
          i_size VARCHAR,
          i_formulation VARCHAR,
          i_color VARCHAR,
          i_units VARCHAR,
          i_container VARCHAR,
          i_manager_id INTEGER,
          i_product_name VARCHAR
        );
        """
    ),

    (
        "promotion",
        """
        -- Snowflake-like DDL for TPC-DS PROMOTION
        CREATE OR REPLACE TABLE {PROMOTION_TABLE} (
          p_promo_sk INTEGER,
          p_promo_id VARCHAR,
          p_start_date_sk INTEGER,
          p_end_date_sk INTEGER,
          p_item_sk INTEGER,
          p_cost DECIMAL(15,
          2),
          p_response_target INTEGER,
          p_promo_name VARCHAR,
          p_channel_dmail VARCHAR,
          p_channel_email VARCHAR,
          p_channel_catalog VARCHAR,
          p_channel_tv VARCHAR,
          p_channel_radio VARCHAR,
          p_channel_press VARCHAR,
          p_channel_event VARCHAR,
          p_channel_demo VARCHAR,
          p_channel_details VARCHAR,
          p_purpose VARCHAR,
          p_discount_active VARCHAR
        );
        """
    ),

    (
        "reason",
        """
        -- Snowflake-like DDL for TPC-DS REASON
        CREATE OR REPLACE TABLE {REASON_TABLE} (
          r_reason_sk INTEGER,
          r_reason_id VARCHAR,
          r_reason_desc VARCHAR
        );
        """
    ),

    (
        "ship_mode",
        """
        -- Snowflake-like DDL for TPC-DS SHIP_MODE
        CREATE OR REPLACE TABLE {SHIP_MODE_TABLE} (
          sm_ship_mode_sk INTEGER,
          sm_ship_mode_id VARCHAR,
          sm_type VARCHAR,
          sm_code VARCHAR,
          sm_carrier VARCHAR,
          sm_contract VARCHAR
        );
        """
    ),

    (
        "store",
        """
        -- Snowflake-like DDL for TPC-DS STORE
        CREATE OR REPLACE TABLE {STORE_TABLE} (
          s_store_sk INTEGER,
          s_store_id VARCHAR,
          s_rec_start_date DATE,
          s_rec_end_date DATE,
          s_closed_date_sk INTEGER,
          s_store_name VARCHAR,
          s_number_employees INTEGER,
          s_floor_space INTEGER,
          s_hours VARCHAR,
          s_manager VARCHAR,
          s_market_id INTEGER,
          s_geography_class VARCHAR,
          s_market_desc VARCHAR,
          s_market_manager VARCHAR,
          s_division_id INTEGER,
          s_division_name VARCHAR,
          s_company_id INTEGER,
          s_company_name VARCHAR,
          s_street_number VARCHAR,
          s_street_name VARCHAR,
          s_street_type VARCHAR,
          s_suite_number VARCHAR,
          s_city VARCHAR,
          s_county VARCHAR,
          s_state VARCHAR,
          s_zip VARCHAR,
          s_country VARCHAR,
          s_gmt_offset DECIMAL(5,
          2),
          s_tax_percentage DECIMAL(5,
          2)
        );
        """
    ),

    (
        "store_returns",
        """
        -- Snowflake-like DDL for TPC-DS STORE_RETURNS
        CREATE OR REPLACE TABLE {STORE_RETURNS_TABLE} (
          sr_returned_date_sk INTEGER,
          sr_return_time_sk INTEGER,
          sr_item_sk INTEGER,
          sr_customer_sk INTEGER,
          sr_cdemo_sk INTEGER,
          sr_hdemo_sk INTEGER,
          sr_addr_sk INTEGER,
          sr_store_sk INTEGER,
          sr_reason_sk INTEGER,
          sr_ticket_number INTEGER,
          sr_return_quantity INTEGER,
          sr_return_amt DECIMAL(7,
          2),
          sr_return_tax DECIMAL(7,
          2),
          sr_return_amt_inc_tax DECIMAL(7,
          2),
          sr_fee DECIMAL(7,
          2),
          sr_return_ship_cost DECIMAL(7,
          2),
          sr_refunded_cash DECIMAL(7,
          2),
          sr_reversed_charge DECIMAL(7,
          2),
          sr_store_credit DECIMAL(7,
          2),
          sr_net_loss DECIMAL(7,
          2)
        );
        """
    ),

    (
        "store_sales",
        """
        -- Snowflake-like DDL for TPC-DS STORE_SALES
        CREATE OR REPLACE TABLE {STORE_SALES_TABLE} (
          ss_sold_date_sk INTEGER,
          ss_sold_time_sk INTEGER,
          ss_item_sk INTEGER,
          ss_customer_sk INTEGER,
          ss_cdemo_sk INTEGER,
          ss_hdemo_sk INTEGER,
          ss_addr_sk INTEGER,
          ss_store_sk INTEGER,
          ss_promo_sk INTEGER,
          ss_ticket_number INTEGER,
          ss_quantity INTEGER,
          ss_wholesale_cost DECIMAL(7,
          2),
          ss_list_price DECIMAL(7,
          2),
          ss_sales_price DECIMAL(7,
          2),
          ss_ext_discount_amt DECIMAL(7,
          2),
          ss_ext_sales_price DECIMAL(7,
          2),
          ss_ext_wholesale_cost DECIMAL(7,
          2),
          ss_ext_list_price DECIMAL(7,
          2),
          ss_ext_tax DECIMAL(7,
          2),
          ss_coupon_amt DECIMAL(7,
          2),
          ss_net_paid DECIMAL(7,
          2),
          ss_net_paid_inc_tax DECIMAL(7,
          2),
          ss_net_profit DECIMAL(7,
          2)
        );
        """
    ),

    (
        "time_dim",
        """
        -- Snowflake-like DDL for TPC-DS TIME_DIM
        CREATE OR REPLACE TABLE {TIME_DIM_TABLE} (
          t_time_sk INTEGER,
          t_time_id VARCHAR,
          t_time INTEGER,
          t_hour INTEGER,
          t_minute INTEGER,
          t_second INTEGER,
          t_am_pm VARCHAR,
          t_shift VARCHAR,
          t_sub_shift VARCHAR,
          t_meal_time VARCHAR
        );
        """
    ),

    (
        "warehouse",
        """
        -- Snowflake-like DDL for TPC-DS WAREHOUSE
        CREATE OR REPLACE TABLE {WAREHOUSE_TABLE} (
          w_warehouse_sk INTEGER,
          w_warehouse_id VARCHAR,
          w_warehouse_name VARCHAR,
          w_warehouse_sq_ft INTEGER,
          w_street_number VARCHAR,
          w_street_name VARCHAR,
          w_street_type VARCHAR,
          w_suite_number VARCHAR,
          w_city VARCHAR,
          w_county VARCHAR,
          w_state VARCHAR,
          w_zip VARCHAR,
          w_country VARCHAR,
          w_gmt_offset DECIMAL(5,
          2)
        );
        """
    ),

    (
        "web_page",
        """
        -- Snowflake-like DDL for TPC-DS WEB_PAGE
        CREATE OR REPLACE TABLE {WEB_PAGE_TABLE} (
          wp_web_page_sk INTEGER,
          wp_web_page_id VARCHAR,
          wp_rec_start_date DATE,
          wp_rec_end_date DATE,
          wp_creation_date_sk INTEGER,
          wp_access_date_sk INTEGER,
          wp_autogen_flag VARCHAR,
          wp_customer_sk INTEGER,
          wp_url VARCHAR,
          wp_type VARCHAR,
          wp_char_count INTEGER,
          wp_link_count INTEGER,
          wp_image_count INTEGER,
          wp_max_ad_count INTEGER
        );
        """
    ),

    (
        "web_returns",
        """
        -- Snowflake-like DDL for TPC-DS WEB_RETURNS
        CREATE OR REPLACE TABLE {WEB_RETURNS_TABLE} (
          wr_returned_date_sk INTEGER,
          wr_returned_time_sk INTEGER,
          wr_item_sk INTEGER,
          wr_refunded_customer_sk INTEGER,
          wr_refunded_cdemo_sk INTEGER,
          wr_refunded_hdemo_sk INTEGER,
          wr_refunded_addr_sk INTEGER,
          wr_returning_customer_sk INTEGER,
          wr_returning_cdemo_sk INTEGER,
          wr_returning_hdemo_sk INTEGER,
          wr_returning_addr_sk INTEGER,
          wr_web_page_sk INTEGER,
          wr_reason_sk INTEGER,
          wr_order_number INTEGER,
          wr_return_quantity INTEGER,
          wr_return_amt DECIMAL(7,
          2),
          wr_return_tax DECIMAL(7,
          2),
          wr_return_amt_inc_tax DECIMAL(7,
          2),
          wr_fee DECIMAL(7,
          2),
          wr_return_ship_cost DECIMAL(7,
          2),
          wr_refunded_cash DECIMAL(7,
          2),
          wr_reversed_charge DECIMAL(7,
          2),
          wr_account_credit DECIMAL(7,
          2),
          wr_net_loss DECIMAL(7,
          2)
        );
        """
    ),

    (
        "web_sales",
        """
        -- Snowflake-like DDL for TPC-DS WEB_SALES
        CREATE OR REPLACE TABLE {WEB_SALES_TABLE} (
          ws_sold_date_sk INTEGER,
          ws_sold_time_sk INTEGER,
          ws_ship_date_sk INTEGER,
          ws_item_sk INTEGER,
          ws_bill_customer_sk INTEGER,
          ws_bill_cdemo_sk INTEGER,
          ws_bill_hdemo_sk INTEGER,
          ws_bill_addr_sk INTEGER,
          ws_ship_customer_sk INTEGER,
          ws_ship_cdemo_sk INTEGER,
          ws_ship_hdemo_sk INTEGER,
          ws_ship_addr_sk INTEGER,
          ws_web_page_sk INTEGER,
          ws_web_site_sk INTEGER,
          ws_ship_mode_sk INTEGER,
          ws_warehouse_sk INTEGER,
          ws_promo_sk INTEGER,
          ws_order_number INTEGER,
          ws_quantity INTEGER,
          ws_wholesale_cost DECIMAL(7,
          2),
          ws_list_price DECIMAL(7,
          2),
          ws_sales_price DECIMAL(7,
          2),
          ws_ext_discount_amt DECIMAL(7,
          2),
          ws_ext_sales_price DECIMAL(7,
          2),
          ws_ext_wholesale_cost DECIMAL(7,
          2),
          ws_ext_list_price DECIMAL(7,
          2),
          ws_ext_tax DECIMAL(7,
          2),
          ws_coupon_amt DECIMAL(7,
          2),
          ws_ext_ship_cost DECIMAL(7,
          2),
          ws_net_paid DECIMAL(7,
          2),
          ws_net_paid_inc_tax DECIMAL(7,
          2),
          ws_net_paid_inc_ship DECIMAL(7,
          2),
          ws_net_paid_inc_ship_tax DECIMAL(7,
          2),
          ws_net_profit DECIMAL(7,
          2)
        );
        """
    ),

    (
        "web_site",
        """
        -- Snowflake-like DDL for TPC-DS WEB_SITE
        CREATE OR REPLACE TABLE {WEB_SITE_TABLE} (
          web_site_sk INTEGER,
          web_site_id VARCHAR,
          web_rec_start_date DATE,
          web_rec_end_date DATE,
          web_name VARCHAR,
          web_open_date_sk INTEGER,
          web_close_date_sk INTEGER,
          web_class VARCHAR,
          web_manager VARCHAR,
          web_mkt_id INTEGER,
          web_mkt_class VARCHAR,
          web_mkt_desc VARCHAR,
          web_market_manager VARCHAR,
          web_company_id INTEGER,
          web_company_name VARCHAR,
          web_street_number VARCHAR,
          web_street_name VARCHAR,
          web_street_type VARCHAR,
          web_suite_number VARCHAR,
          web_city VARCHAR,
          web_county VARCHAR,
          web_state VARCHAR,
          web_zip VARCHAR,
          web_country VARCHAR,
          web_gmt_offset DECIMAL(5,
          2),
          web_tax_percentage DECIMAL(5,
          2)
        );
        """
    ),
]


def parametrize_tpcds_ddl(fully_qualified_names_for_embucket):
    """
    Replace table name placeholders in TPC-H DDL statements with actual table names.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use just the default table names.

    Returns:
        list: A list of (table_name, parametrized_ddl) tuples.
    """
    return parametrize_tpcds_statements(_TPCDS_DDL_RAW, fully_qualified_names_for_embucket)