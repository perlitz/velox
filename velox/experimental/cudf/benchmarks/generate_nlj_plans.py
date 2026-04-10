#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate synthetic Velox query plans exercising NestedLoopJoin.

Produces Q1.json ... Q6.json that can be fed directly to the TPC-DS benchmark
via --plan_path, using real TPC-DS parquet tables as data sources.

Usage:
    python3 generate_nlj_plans.py --output_dir=/velox/VeloxPlans/synthetic/nlj
"""

import argparse
import json
import os


# ---------------------------------------------------------------------------
# Node ID allocator — reset per query
# ---------------------------------------------------------------------------

_next_id = 0


def reset_ids():
    global _next_id
    _next_id = 0


def next_id():
    global _next_id
    _next_id += 1
    return str(_next_id)


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------


_TYPE_NAME_OVERRIDES = {
    "DATE": "DateType",
}


def make_type(velox_type):
    """Velox scalar type: INTEGER, BIGINT, DOUBLE, VARCHAR, BOOLEAN, DATE."""
    name = _TYPE_NAME_OVERRIDES.get(velox_type, "Type")
    return {"name": name, "type": velox_type}


def make_row_type(names, types):
    """ROW(...) type used in outputType fields."""
    return {
        "cTypes": [make_type(t) for t in types],
        "name": "Type",
        "names": list(names),
        "type": "ROW",
    }


# ---------------------------------------------------------------------------
# Full TPC-DS table schemas (from SF1 parquet files).
# dataColumns in TableScanNode must list ALL columns in the table, not just the
# columns being read, because the parquet reader uses ordinal position mapping.
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = {
    "store_sales": [
        ("ss_sold_date_sk", "INTEGER"),
        ("ss_sold_time_sk", "INTEGER"),
        ("ss_item_sk", "INTEGER"),
        ("ss_customer_sk", "INTEGER"),
        ("ss_cdemo_sk", "INTEGER"),
        ("ss_hdemo_sk", "INTEGER"),
        ("ss_addr_sk", "INTEGER"),
        ("ss_store_sk", "INTEGER"),
        ("ss_promo_sk", "INTEGER"),
        ("ss_ticket_number", "INTEGER"),
        ("ss_quantity", "INTEGER"),
        ("ss_wholesale_cost", "DOUBLE"),
        ("ss_list_price", "DOUBLE"),
        ("ss_sales_price", "DOUBLE"),
        ("ss_ext_discount_amt", "DOUBLE"),
        ("ss_ext_sales_price", "DOUBLE"),
        ("ss_ext_wholesale_cost", "DOUBLE"),
        ("ss_ext_list_price", "DOUBLE"),
        ("ss_ext_tax", "DOUBLE"),
        ("ss_coupon_amt", "DOUBLE"),
        ("ss_net_paid", "DOUBLE"),
        ("ss_net_paid_inc_tax", "DOUBLE"),
        ("ss_net_profit", "DOUBLE"),
    ],
    "item": [
        ("i_item_sk", "INTEGER"),
        ("i_item_id", "VARCHAR"),
        ("i_rec_start_date", "DATE"),
        ("i_rec_end_date", "DATE"),
        ("i_item_desc", "VARCHAR"),
        ("i_current_price", "DOUBLE"),
        ("i_wholesale_cost", "DOUBLE"),
        ("i_brand_id", "INTEGER"),
        ("i_brand", "VARCHAR"),
        ("i_class_id", "INTEGER"),
        ("i_class", "VARCHAR"),
        ("i_category_id", "INTEGER"),
        ("i_category", "VARCHAR"),
        ("i_manufact_id", "INTEGER"),
        ("i_manufact", "VARCHAR"),
        ("i_size", "VARCHAR"),
        ("i_formulation", "VARCHAR"),
        ("i_color", "VARCHAR"),
        ("i_units", "VARCHAR"),
        ("i_container", "VARCHAR"),
        ("i_manager_id", "INTEGER"),
        ("i_product_name", "VARCHAR"),
    ],
    "date_dim": [
        ("d_date_sk", "INTEGER"),
        ("d_date_id", "VARCHAR"),
        ("d_date", "DATE"),
        ("d_month_seq", "INTEGER"),
        ("d_week_seq", "INTEGER"),
        ("d_quarter_seq", "INTEGER"),
        ("d_year", "INTEGER"),
        ("d_dow", "INTEGER"),
        ("d_moy", "INTEGER"),
        ("d_dom", "INTEGER"),
        ("d_qoy", "INTEGER"),
        ("d_fy_year", "INTEGER"),
        ("d_fy_quarter_seq", "INTEGER"),
        ("d_fy_week_seq", "INTEGER"),
        ("d_day_name", "VARCHAR"),
        ("d_quarter_name", "VARCHAR"),
        ("d_holiday", "VARCHAR"),
        ("d_weekend", "VARCHAR"),
        ("d_following_holiday", "VARCHAR"),
        ("d_first_dom", "INTEGER"),
        ("d_last_dom", "INTEGER"),
        ("d_same_day_ly", "INTEGER"),
        ("d_same_day_lq", "INTEGER"),
        ("d_current_day", "VARCHAR"),
        ("d_current_week", "VARCHAR"),
        ("d_current_month", "VARCHAR"),
        ("d_current_quarter", "VARCHAR"),
        ("d_current_year", "VARCHAR"),
    ],
    "catalog_sales": [
        ("cs_sold_date_sk", "INTEGER"),
        ("cs_sold_time_sk", "INTEGER"),
        ("cs_ship_date_sk", "INTEGER"),
        ("cs_bill_customer_sk", "INTEGER"),
        ("cs_bill_cdemo_sk", "INTEGER"),
        ("cs_bill_hdemo_sk", "INTEGER"),
        ("cs_bill_addr_sk", "INTEGER"),
        ("cs_ship_customer_sk", "INTEGER"),
        ("cs_ship_cdemo_sk", "INTEGER"),
        ("cs_ship_hdemo_sk", "INTEGER"),
        ("cs_ship_addr_sk", "INTEGER"),
        ("cs_call_center_sk", "INTEGER"),
        ("cs_catalog_page_sk", "INTEGER"),
        ("cs_ship_mode_sk", "INTEGER"),
        ("cs_warehouse_sk", "INTEGER"),
        ("cs_item_sk", "INTEGER"),
        ("cs_promo_sk", "INTEGER"),
        ("cs_order_number", "INTEGER"),
        ("cs_quantity", "INTEGER"),
        ("cs_wholesale_cost", "DOUBLE"),
        ("cs_list_price", "DOUBLE"),
        ("cs_sales_price", "DOUBLE"),
        ("cs_ext_discount_amt", "DOUBLE"),
        ("cs_ext_sales_price", "DOUBLE"),
        ("cs_ext_wholesale_cost", "DOUBLE"),
        ("cs_ext_list_price", "DOUBLE"),
        ("cs_ext_tax", "DOUBLE"),
        ("cs_coupon_amt", "DOUBLE"),
        ("cs_ext_ship_cost", "DOUBLE"),
        ("cs_net_paid", "DOUBLE"),
        ("cs_net_paid_inc_tax", "DOUBLE"),
        ("cs_net_paid_inc_ship", "DOUBLE"),
        ("cs_net_paid_inc_ship_tax", "DOUBLE"),
        ("cs_net_profit", "DOUBLE"),
    ],
    "store": [
        ("s_store_sk", "INTEGER"),
        ("s_store_id", "VARCHAR"),
        ("s_rec_start_date", "DATE"),
        ("s_rec_end_date", "DATE"),
        ("s_closed_date_sk", "INTEGER"),
        ("s_store_name", "VARCHAR"),
        ("s_number_employees", "INTEGER"),
        ("s_floor_space", "INTEGER"),
        ("s_hours", "VARCHAR"),
        ("s_manager", "VARCHAR"),
        ("s_market_id", "INTEGER"),
        ("s_geography_class", "VARCHAR"),
        ("s_market_desc", "VARCHAR"),
        ("s_market_manager", "VARCHAR"),
        ("s_division_id", "INTEGER"),
        ("s_division_name", "VARCHAR"),
        ("s_company_id", "INTEGER"),
        ("s_company_name", "VARCHAR"),
        ("s_street_number", "VARCHAR"),
        ("s_street_name", "VARCHAR"),
        ("s_street_type", "VARCHAR"),
        ("s_suite_number", "VARCHAR"),
        ("s_city", "VARCHAR"),
        ("s_county", "VARCHAR"),
        ("s_state", "VARCHAR"),
        ("s_zip", "VARCHAR"),
        ("s_country", "VARCHAR"),
        ("s_gmt_offset", "DOUBLE"),
        ("s_tax_percentage", "DOUBLE"),
    ],
    "customer": [
        ("c_customer_sk", "INTEGER"),
        ("c_customer_id", "VARCHAR"),
        ("c_current_cdemo_sk", "INTEGER"),
        ("c_current_hdemo_sk", "INTEGER"),
        ("c_current_addr_sk", "INTEGER"),
        ("c_first_shipto_date_sk", "INTEGER"),
        ("c_first_sales_date_sk", "INTEGER"),
        ("c_salutation", "VARCHAR"),
        ("c_first_name", "VARCHAR"),
        ("c_last_name", "VARCHAR"),
        ("c_preferred_cust_flag", "VARCHAR"),
        ("c_birth_day", "INTEGER"),
        ("c_birth_month", "INTEGER"),
        ("c_birth_year", "INTEGER"),
        ("c_birth_country", "VARCHAR"),
        ("c_login", "VARCHAR"),
        ("c_email_address", "VARCHAR"),
        ("c_last_review_date_sk", "INTEGER"),
    ],
    "customer_address": [
        ("ca_address_sk", "INTEGER"),
        ("ca_address_id", "VARCHAR"),
        ("ca_street_number", "VARCHAR"),
        ("ca_street_name", "VARCHAR"),
        ("ca_street_type", "VARCHAR"),
        ("ca_suite_number", "VARCHAR"),
        ("ca_city", "VARCHAR"),
        ("ca_county", "VARCHAR"),
        ("ca_state", "VARCHAR"),
        ("ca_zip", "VARCHAR"),
        ("ca_country", "VARCHAR"),
        ("ca_gmt_offset", "DOUBLE"),
        ("ca_location_type", "VARCHAR"),
    ],
    "web_sales": [
        ("ws_sold_date_sk", "INTEGER"),
        ("ws_sold_time_sk", "INTEGER"),
        ("ws_ship_date_sk", "INTEGER"),
        ("ws_item_sk", "INTEGER"),
        ("ws_bill_customer_sk", "INTEGER"),
        ("ws_bill_cdemo_sk", "INTEGER"),
        ("ws_bill_hdemo_sk", "INTEGER"),
        ("ws_bill_addr_sk", "INTEGER"),
        ("ws_ship_customer_sk", "INTEGER"),
        ("ws_ship_cdemo_sk", "INTEGER"),
        ("ws_ship_hdemo_sk", "INTEGER"),
        ("ws_ship_addr_sk", "INTEGER"),
        ("ws_web_page_sk", "INTEGER"),
        ("ws_web_site_sk", "INTEGER"),
        ("ws_ship_mode_sk", "INTEGER"),
        ("ws_warehouse_sk", "INTEGER"),
        ("ws_promo_sk", "INTEGER"),
        ("ws_order_number", "INTEGER"),
        ("ws_quantity", "INTEGER"),
        ("ws_wholesale_cost", "DOUBLE"),
        ("ws_list_price", "DOUBLE"),
        ("ws_sales_price", "DOUBLE"),
        ("ws_ext_discount_amt", "DOUBLE"),
        ("ws_ext_sales_price", "DOUBLE"),
        ("ws_ext_wholesale_cost", "DOUBLE"),
        ("ws_ext_list_price", "DOUBLE"),
        ("ws_ext_tax", "DOUBLE"),
        ("ws_coupon_amt", "DOUBLE"),
        ("ws_ext_ship_cost", "DOUBLE"),
        ("ws_net_paid", "DOUBLE"),
        ("ws_net_paid_inc_tax", "DOUBLE"),
        ("ws_net_paid_inc_ship", "DOUBLE"),
        ("ws_net_paid_inc_ship_tax", "DOUBLE"),
        ("ws_net_profit", "DOUBLE"),
    ],
    "promotion": [
        ("p_promo_sk", "INTEGER"),
        ("p_promo_id", "VARCHAR"),
        ("p_start_date_sk", "INTEGER"),
        ("p_end_date_sk", "INTEGER"),
        ("p_item_sk", "INTEGER"),
        ("p_cost", "DOUBLE"),
        ("p_response_target", "INTEGER"),
        ("p_promo_name", "VARCHAR"),
        ("p_channel_dmail", "VARCHAR"),
        ("p_channel_email", "VARCHAR"),
        ("p_channel_catalog", "VARCHAR"),
        ("p_channel_tv", "VARCHAR"),
        ("p_channel_radio", "VARCHAR"),
        ("p_channel_press", "VARCHAR"),
        ("p_channel_event", "VARCHAR"),
        ("p_channel_demo", "VARCHAR"),
        ("p_channel_details", "VARCHAR"),
        ("p_purpose", "VARCHAR"),
        ("p_discount_active", "VARCHAR"),
    ],
    "warehouse": [
        ("w_warehouse_sk", "INTEGER"),
        ("w_warehouse_id", "VARCHAR"),
        ("w_warehouse_name", "VARCHAR"),
        ("w_warehouse_sq_ft", "INTEGER"),
        ("w_street_number", "VARCHAR"),
        ("w_street_name", "VARCHAR"),
        ("w_street_type", "VARCHAR"),
        ("w_suite_number", "VARCHAR"),
        ("w_city", "VARCHAR"),
        ("w_county", "VARCHAR"),
        ("w_state", "VARCHAR"),
        ("w_zip", "VARCHAR"),
        ("w_country", "VARCHAR"),
        ("w_gmt_offset", "DOUBLE"),
    ],
    "web_page": [
        ("wp_web_page_sk", "INTEGER"),
        ("wp_web_page_id", "VARCHAR"),
        ("wp_rec_start_date", "DATE"),
        ("wp_rec_end_date", "DATE"),
        ("wp_creation_date_sk", "INTEGER"),
        ("wp_access_date_sk", "INTEGER"),
        ("wp_autogen_flag", "VARCHAR"),
        ("wp_customer_sk", "INTEGER"),
        ("wp_url", "VARCHAR"),
        ("wp_type", "VARCHAR"),
        ("wp_char_count", "INTEGER"),
        ("wp_link_count", "INTEGER"),
        ("wp_image_count", "INTEGER"),
        ("wp_max_ad_count", "INTEGER"),
    ],
    "catalog_page": [
        ("cp_catalog_page_sk", "INTEGER"),
        ("cp_catalog_page_id", "VARCHAR"),
        ("cp_start_date_sk", "INTEGER"),
        ("cp_end_date_sk", "INTEGER"),
        ("cp_department", "VARCHAR"),
        ("cp_catalog_number", "INTEGER"),
        ("cp_catalog_page_number", "INTEGER"),
        ("cp_description", "VARCHAR"),
        ("cp_type", "VARCHAR"),
    ],
    "time_dim": [
        ("t_time_sk", "INTEGER"),
        ("t_time_id", "VARCHAR"),
        ("t_time", "INTEGER"),
        ("t_hour", "INTEGER"),
        ("t_minute", "INTEGER"),
        ("t_second", "INTEGER"),
        ("t_am_pm", "VARCHAR"),
        ("t_shift", "VARCHAR"),
        ("t_sub_shift", "VARCHAR"),
        ("t_meal_time", "VARCHAR"),
    ],
    "household_demographics": [
        ("hd_demo_sk", "INTEGER"),
        ("hd_income_band_sk", "INTEGER"),
        ("hd_buy_potential", "VARCHAR"),
        ("hd_dep_count", "INTEGER"),
        ("hd_vehicle_count", "INTEGER"),
    ],
}


# ---------------------------------------------------------------------------
# Expression helpers (ITypedExpr serialization)
# ---------------------------------------------------------------------------


def make_field_access(field_name, velox_type):
    """FieldAccessTypedExpr — references a column by name."""
    return {
        "fieldName": field_name,
        "name": "FieldAccessTypedExpr",
        "type": make_type(velox_type),
    }


def make_constant(value, velox_type):
    """ConstantTypedExpr — a literal value."""
    return {
        "name": "ConstantTypedExpr",
        "type": make_type(velox_type),
        "value": {"type": velox_type, "value": value},
    }


def make_call(func_name, inputs, return_type):
    """CallTypedExpr — a function call with presto.default. prefix."""
    return {
        "functionName": f"presto.default.{func_name}",
        "inputs": inputs,
        "name": "CallTypedExpr",
        "type": make_type(return_type),
    }


def make_and(left, right):
    """AND two boolean expressions.

    AND/OR are special forms in Velox, not regular presto.default. functions.
    """
    return {
        "functionName": "and",
        "inputs": [left, right],
        "name": "CallTypedExpr",
        "type": make_type("BOOLEAN"),
    }


def make_between(value, low, high):
    """BETWEEN expression: value >= low AND value <= high."""
    return {
        "functionName": "presto.default.between",
        "inputs": [value, low, high],
        "name": "CallTypedExpr",
        "type": make_type("BOOLEAN"),
    }


def make_cast(input_expr, target_type):
    """CAST expression."""
    return {
        "functionName": "presto.default.cast",
        "inputs": [input_expr],
        "name": "CastTypedExpr",
        "nullOnFailure": False,
        "type": make_type(target_type),
    }


# ---------------------------------------------------------------------------
# Plan node helpers
# ---------------------------------------------------------------------------


def make_column_handle(col_name, velox_type):
    """HiveColumnHandle for a regular column."""
    return {
        "columnType": "Regular",
        "dataType": make_type(velox_type),
        "hiveColumnHandleName": col_name,
        "hiveType": make_type(velox_type),
        "name": "HiveColumnHandle",
        "requiredSubfields": [],
    }


def make_table_scan(table_name, columns, num_rows=0):
    """TableScanNode reading specified columns from a TPC-DS table.

    Args:
        table_name: e.g. "store_sales" (will be prefixed with tpcds_sf100.)
        columns: list of (alias, hive_col_name, velox_type) tuples
        num_rows: approximate row count for tableParameters
    """
    node_id = next_id()
    full_table_name = f"tpcds_sf100.{table_name}"

    assignments = []
    output_names = []
    output_types = []

    for alias, hive_name, vtype in columns:
        assignments.append(
            {"assign": alias, "columnHandle": make_column_handle(hive_name, vtype)}
        )
        output_names.append(alias)
        output_types.append(vtype)

    # dataColumns must be the full table schema so the parquet reader can
    # map columns by ordinal position correctly.
    schema = TABLE_SCHEMAS[table_name]
    data_col_names = [name for name, _ in schema]
    data_col_types = [vtype for _, vtype in schema]

    return {
        "assignments": assignments,
        "id": node_id,
        "name": "TableScanNode",
        "outputType": make_row_type(output_names, output_types),
        "tableHandle": {
            "connectorId": "hive",
            "dataColumns": make_row_type(data_col_names, data_col_types),
            "filterPushdownEnabled": False,
            "name": "HiveTableHandle",
            "subfieldFilters": [],
            "tableName": full_table_name,
            "tableParameters": {
                "EXTERNAL": "TRUE",
                "numFiles": "0",
                "numRows": str(num_rows),
                "totalSize": "0",
            },
        },
    }


def make_filter(source, condition):
    """FilterNode wrapping a source with a boolean condition."""
    node_id = next_id()
    return {
        "filter": condition,
        "id": node_id,
        "name": "FilterNode",
        "outputType": source["outputType"],
        "sources": [source],
    }


def make_local_partition_gather(source):
    """LocalPartitionNode (Gather) wrapping the build side."""
    outer_id = next_id()
    inner_id = f"{outer_id}.0"

    # The inner source is a ProjectNode that passes through all columns.
    out_type = source["outputType"]
    names = out_type["names"]
    types = [ct["type"] for ct in out_type["cTypes"]]

    projections = [make_field_access(n, t) for n, t in zip(names, types)]

    project = {
        "id": inner_id,
        "name": "ProjectNode",
        "names": list(names),
        "projections": projections,
        "sources": [source],
    }

    return {
        "id": outer_id,
        "name": "LocalPartitionNode",
        "type": "GATHER",
        "partitionFunctionSpec": {"name": "GatherPartitionFunctionSpec"},
        "scaleWriter": False,
        "sources": [project],
    }


def make_nlj(join_condition, probe, build, output_columns):
    """NestedLoopJoinNode (INNER).

    Args:
        join_condition: expression dict or None for cross join
        probe: source[0] plan node
        build: source[1] plan node (will be wrapped in LocalPartitionNode)
        output_columns: list of (name, velox_type) for the output
    """
    node_id = next_id()
    names = [c[0] for c in output_columns]
    types = [c[1] for c in output_columns]

    build_wrapped = make_local_partition_gather(build)

    node = {
        "id": node_id,
        "joinType": "INNER",
        "name": "NestedLoopJoinNode",
        "outputType": make_row_type(names, types),
        "sources": [probe, build_wrapped],
    }
    if join_condition is not None:
        node["joinCondition"] = join_condition
    return node


def make_partitioned_output(source):
    """PartitionedOutputNode — top-level plan wrapper."""
    node_id = next_id()
    return {
        "id": node_id,
        "keys": [],
        "kind": "PARTITIONED",
        "name": "PartitionedOutputNode",
        "numPartitions": 1,
        "outputType": source["outputType"],
        "partitionFunctionSpec": {"name": "GatherPartitionFunctionSpec"},
        "replicateNullsAndAny": False,
        "serdeKind": "Presto",
        "sources": [source],
    }


# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------


def query_1():
    """Range join: store_sales x item on price range.

    ss_list_price BETWEEN (i_current_price - 1.0) AND (i_current_price + 1.0)
    """
    probe = make_table_scan(
        "store_sales",
        [
            ("ss_item_sk", "ss_item_sk", "INTEGER"),
            ("ss_list_price", "ss_list_price", "DOUBLE"),
            ("ss_sales_price", "ss_sales_price", "DOUBLE"),
        ],
        num_rows=2880404,
    )
    build = make_table_scan(
        "item",
        [
            ("i_item_sk", "i_item_sk", "INTEGER"),
            ("i_current_price", "i_current_price", "DOUBLE"),
            ("i_item_desc", "i_item_desc", "VARCHAR"),
        ],
        num_rows=18000,
    )

    condition = make_between(
        make_field_access("ss_list_price", "DOUBLE"),
        make_call(
            "minus",
            [
                make_field_access("i_current_price", "DOUBLE"),
                make_constant(1.0, "DOUBLE"),
            ],
            "DOUBLE",
        ),
        make_call(
            "plus",
            [
                make_field_access("i_current_price", "DOUBLE"),
                make_constant(1.0, "DOUBLE"),
            ],
            "DOUBLE",
        ),
    )

    output_columns = [
        ("ss_item_sk", "INTEGER"),
        ("ss_list_price", "DOUBLE"),
        ("ss_sales_price", "DOUBLE"),
        ("i_item_sk", "INTEGER"),
        ("i_current_price", "DOUBLE"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_2():
    """Inequality join with filtered build: store_sales x date_dim.

    Build side filtered to a single year (d_year = 2000), producing ~365 rows.
    Condition: ss_sold_date_sk > d_date_sk
    """
    probe = make_table_scan(
        "store_sales",
        [
            ("ss_sold_date_sk", "ss_sold_date_sk", "INTEGER"),
            ("ss_ext_sales_price", "ss_ext_sales_price", "DOUBLE"),
        ],
        num_rows=2880404,
    )
    build_scan = make_table_scan(
        "date_dim",
        [
            ("d_date_sk", "d_date_sk", "INTEGER"),
            ("d_year", "d_year", "INTEGER"),
        ],
        num_rows=73049,
    )
    build = make_filter(
        build_scan,
        make_call(
            "eq",
            [make_field_access("d_year", "INTEGER"), make_constant(2000, "INTEGER")],
            "BOOLEAN",
        ),
    )

    condition = make_call(
        "gt",
        [
            make_field_access("ss_sold_date_sk", "INTEGER"),
            make_field_access("d_date_sk", "INTEGER"),
        ],
        "BOOLEAN",
    )

    output_columns = [
        ("ss_sold_date_sk", "INTEGER"),
        ("ss_ext_sales_price", "DOUBLE"),
        ("d_date_sk", "INTEGER"),
        ("d_year", "INTEGER"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_3():
    """Multi-condition join: catalog_sales x item.

    cs_list_price > i_current_price AND cs_wholesale_cost < i_wholesale_cost
    """
    probe = make_table_scan(
        "catalog_sales",
        [
            ("cs_item_sk", "cs_item_sk", "INTEGER"),
            ("cs_list_price", "cs_list_price", "DOUBLE"),
            ("cs_wholesale_cost", "cs_wholesale_cost", "DOUBLE"),
        ],
        num_rows=1441548,
    )
    build = make_table_scan(
        "item",
        [
            ("i_item_sk", "i_item_sk", "INTEGER"),
            ("i_current_price", "i_current_price", "DOUBLE"),
            ("i_wholesale_cost", "i_wholesale_cost", "DOUBLE"),
        ],
        num_rows=18000,
    )

    condition = make_and(
        make_call(
            "gt",
            [
                make_field_access("cs_list_price", "DOUBLE"),
                make_field_access("i_current_price", "DOUBLE"),
            ],
            "BOOLEAN",
        ),
        make_call(
            "lt",
            [
                make_field_access("cs_wholesale_cost", "DOUBLE"),
                make_field_access("i_wholesale_cost", "DOUBLE"),
            ],
            "BOOLEAN",
        ),
    )

    output_columns = [
        ("cs_item_sk", "INTEGER"),
        ("cs_list_price", "DOUBLE"),
        ("cs_wholesale_cost", "DOUBLE"),
        ("i_item_sk", "INTEGER"),
        ("i_current_price", "DOUBLE"),
        ("i_wholesale_cost", "DOUBLE"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_4():
    """Small x small baseline: store x item.

    Simple condition: i_current_price > 50.0
    store has 12 rows, item has 18K — cross product is only 216K, filtered ~108K.
    """
    probe = make_table_scan(
        "store",
        [
            ("s_store_sk", "s_store_sk", "INTEGER"),
            ("s_store_name", "s_store_name", "VARCHAR"),
            ("s_number_employees", "s_number_employees", "INTEGER"),
        ],
        num_rows=12,
    )
    build = make_table_scan(
        "item",
        [
            ("i_item_sk", "i_item_sk", "INTEGER"),
            ("i_current_price", "i_current_price", "DOUBLE"),
        ],
        num_rows=18000,
    )

    condition = make_call(
        "gt",
        [
            make_field_access("i_current_price", "DOUBLE"),
            make_constant(50.0, "DOUBLE"),
        ],
        "BOOLEAN",
    )

    output_columns = [
        ("s_store_sk", "INTEGER"),
        ("s_store_name", "VARCHAR"),
        ("i_item_sk", "INTEGER"),
        ("i_current_price", "DOUBLE"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_5():
    """Medium cross-product: customer x customer_address with birth year filter.

    Probe filtered to c_birth_year > 1970 (~30K rows from 100K).
    Build is all customer_address (50K rows).
    Condition: c_current_addr_sk > ca_address_sk (inequality to avoid hash join).
    Output: ~hundreds of millions after cross-product with condition.
    """
    probe_scan = make_table_scan(
        "customer",
        [
            ("c_customer_sk", "c_customer_sk", "INTEGER"),
            ("c_birth_year", "c_birth_year", "INTEGER"),
            ("c_current_addr_sk", "c_current_addr_sk", "INTEGER"),
        ],
        num_rows=100000,
    )
    probe = make_filter(
        probe_scan,
        make_call(
            "gt",
            [
                make_field_access("c_birth_year", "INTEGER"),
                make_constant(1970, "INTEGER"),
            ],
            "BOOLEAN",
        ),
    )

    build = make_table_scan(
        "customer_address",
        [
            ("ca_address_sk", "ca_address_sk", "INTEGER"),
            ("ca_state", "ca_state", "VARCHAR"),
        ],
        num_rows=50000,
    )

    condition = make_call(
        "gt",
        [
            make_field_access("c_current_addr_sk", "INTEGER"),
            make_field_access("ca_address_sk", "INTEGER"),
        ],
        "BOOLEAN",
    )

    output_columns = [
        ("c_customer_sk", "INTEGER"),
        ("c_current_addr_sk", "INTEGER"),
        ("ca_address_sk", "INTEGER"),
        ("ca_state", "VARCHAR"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_6():
    """Fact-to-fact theta join: web_sales x store_sales (filtered).

    Build: store_sales filtered to ss_store_sk = 1 (~30K rows at SF1).
    Condition: ws_ext_sales_price > ss_ext_sales_price
    """
    probe = make_table_scan(
        "web_sales",
        [
            ("ws_item_sk", "ws_item_sk", "INTEGER"),
            ("ws_ext_sales_price", "ws_ext_sales_price", "DOUBLE"),
        ],
        num_rows=719384,
    )
    build_scan = make_table_scan(
        "store_sales",
        [
            ("ss_item_sk", "ss_item_sk", "INTEGER"),
            ("ss_store_sk", "ss_store_sk", "INTEGER"),
            ("ss_ext_sales_price", "ss_ext_sales_price", "DOUBLE"),
        ],
        num_rows=2880404,
    )
    build = make_filter(
        build_scan,
        make_call(
            "eq",
            [
                make_field_access("ss_store_sk", "INTEGER"),
                make_constant(1, "INTEGER"),
            ],
            "BOOLEAN",
        ),
    )

    condition = make_call(
        "gt",
        [
            make_field_access("ws_ext_sales_price", "DOUBLE"),
            make_field_access("ss_ext_sales_price", "DOUBLE"),
        ],
        "BOOLEAN",
    )

    output_columns = [
        ("ws_item_sk", "INTEGER"),
        ("ws_ext_sales_price", "DOUBLE"),
        ("ss_item_sk", "INTEGER"),
        ("ss_ext_sales_price", "DOUBLE"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_7():
    """SF100-safe: store x promotion on date range overlap.

    Probe: store (402 at SF100), Build: promotion (1K at SF100).
    Cross-product: ~402K pairs. Tests BETWEEN with arithmetic on both sides.
    Condition: p_start_date_sk BETWEEN (s_closed_date_sk - 100) AND
               (s_closed_date_sk + 100)
    """
    probe = make_table_scan(
        "store",
        [
            ("s_store_sk", "s_store_sk", "INTEGER"),
            ("s_store_name", "s_store_name", "VARCHAR"),
            ("s_closed_date_sk", "s_closed_date_sk", "INTEGER"),
        ],
        num_rows=402,
    )
    build = make_table_scan(
        "promotion",
        [
            ("p_promo_sk", "p_promo_sk", "INTEGER"),
            ("p_promo_name", "p_promo_name", "VARCHAR"),
            ("p_start_date_sk", "p_start_date_sk", "INTEGER"),
            ("p_cost", "p_cost", "DOUBLE"),
        ],
        num_rows=1000,
    )

    condition = make_between(
        make_field_access("p_start_date_sk", "INTEGER"),
        make_call(
            "minus",
            [
                make_field_access("s_closed_date_sk", "INTEGER"),
                make_constant(100, "INTEGER"),
            ],
            "INTEGER",
        ),
        make_call(
            "plus",
            [
                make_field_access("s_closed_date_sk", "INTEGER"),
                make_constant(100, "INTEGER"),
            ],
            "INTEGER",
        ),
    )

    output_columns = [
        ("s_store_sk", "INTEGER"),
        ("s_store_name", "VARCHAR"),
        ("p_promo_sk", "INTEGER"),
        ("p_promo_name", "VARCHAR"),
        ("p_cost", "DOUBLE"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_8():
    """SF100-safe: date_dim (1 year) x store with inequality.

    Probe: date_dim filtered to d_year=2000 (~365 rows).
    Build: store (402 at SF100).
    Cross-product: ~147K pairs. Tests filtered probe + simple inequality.
    Condition: d_date_sk > s_store_sk
    """
    probe_scan = make_table_scan(
        "date_dim",
        [
            ("d_date_sk", "d_date_sk", "INTEGER"),
            ("d_year", "d_year", "INTEGER"),
            ("d_day_name", "d_day_name", "VARCHAR"),
        ],
        num_rows=73049,
    )
    probe = make_filter(
        probe_scan,
        make_call(
            "eq",
            [make_field_access("d_year", "INTEGER"), make_constant(2000, "INTEGER")],
            "BOOLEAN",
        ),
    )
    build = make_table_scan(
        "store",
        [
            ("s_store_sk", "s_store_sk", "INTEGER"),
            ("s_store_name", "s_store_name", "VARCHAR"),
            ("s_tax_percentage", "s_tax_percentage", "DOUBLE"),
        ],
        num_rows=402,
    )

    condition = make_call(
        "gt",
        [
            make_field_access("d_date_sk", "INTEGER"),
            make_field_access("s_store_sk", "INTEGER"),
        ],
        "BOOLEAN",
    )

    output_columns = [
        ("d_date_sk", "INTEGER"),
        ("d_day_name", "VARCHAR"),
        ("s_store_sk", "INTEGER"),
        ("s_store_name", "VARCHAR"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_9():
    """SF100-safe: web_page x catalog_page with multi-condition AND.

    Probe: web_page (2K at SF100). Build: catalog_page (20K at SF100).
    Cross-product: ~40M pairs. Tests AND of two inequalities on dimension
    tables. This is the largest SF100-safe query.
    Condition: wp_web_page_sk > cp_catalog_page_sk AND
               wp_char_count > cp_catalog_page_number
    """
    probe = make_table_scan(
        "web_page",
        [
            ("wp_web_page_sk", "wp_web_page_sk", "INTEGER"),
            ("wp_char_count", "wp_char_count", "INTEGER"),
            ("wp_link_count", "wp_link_count", "INTEGER"),
        ],
        num_rows=2040,
    )
    build = make_table_scan(
        "catalog_page",
        [
            ("cp_catalog_page_sk", "cp_catalog_page_sk", "INTEGER"),
            ("cp_catalog_page_number", "cp_catalog_page_number", "INTEGER"),
            ("cp_catalog_number", "cp_catalog_number", "INTEGER"),
        ],
        num_rows=20400,
    )

    condition = make_and(
        make_call(
            "gt",
            [
                make_field_access("wp_web_page_sk", "INTEGER"),
                make_field_access("cp_catalog_page_sk", "INTEGER"),
            ],
            "BOOLEAN",
        ),
        make_call(
            "gt",
            [
                make_field_access("wp_char_count", "INTEGER"),
                make_field_access("cp_catalog_page_number", "INTEGER"),
            ],
            "BOOLEAN",
        ),
    )

    output_columns = [
        ("wp_web_page_sk", "INTEGER"),
        ("wp_char_count", "INTEGER"),
        ("cp_catalog_page_sk", "INTEGER"),
        ("cp_catalog_page_number", "INTEGER"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


def query_10():
    """SF100-safe: item (filtered) x household_demographics.

    Probe: item filtered to i_category_id=1 (~15K rows at SF100).
    Build: household_demographics (7.2K at SF100).
    Cross-product: ~108M pairs. Tests filtered probe + BETWEEN on doubles.
    Condition: i_current_price BETWEEN 10.0 AND 50.0 (always evaluated per pair)
    AND hd_dep_count > 0
    """
    probe_scan = make_table_scan(
        "item",
        [
            ("i_item_sk", "i_item_sk", "INTEGER"),
            ("i_current_price", "i_current_price", "DOUBLE"),
            ("i_category_id", "i_category_id", "INTEGER"),
        ],
        num_rows=204000,
    )
    probe = make_filter(
        probe_scan,
        make_call(
            "eq",
            [
                make_field_access("i_category_id", "INTEGER"),
                make_constant(1, "INTEGER"),
            ],
            "BOOLEAN",
        ),
    )
    build = make_table_scan(
        "household_demographics",
        [
            ("hd_demo_sk", "hd_demo_sk", "INTEGER"),
            ("hd_dep_count", "hd_dep_count", "INTEGER"),
            ("hd_vehicle_count", "hd_vehicle_count", "INTEGER"),
        ],
        num_rows=7200,
    )

    condition = make_and(
        make_between(
            make_field_access("i_current_price", "DOUBLE"),
            make_constant(10.0, "DOUBLE"),
            make_constant(50.0, "DOUBLE"),
        ),
        make_call(
            "gt",
            [
                make_field_access("hd_dep_count", "INTEGER"),
                make_constant(0, "INTEGER"),
            ],
            "BOOLEAN",
        ),
    )

    output_columns = [
        ("i_item_sk", "INTEGER"),
        ("i_current_price", "DOUBLE"),
        ("hd_demo_sk", "INTEGER"),
        ("hd_dep_count", "INTEGER"),
    ]

    nlj = make_nlj(condition, probe, build, output_columns)
    return make_partitioned_output(nlj)


# ---------------------------------------------------------------------------
# Query registry
# ---------------------------------------------------------------------------

QUERIES = {
    1: ("Range join: store_sales x item on price band", query_1),
    2: ("Inequality + filtered build: store_sales x date_dim", query_2),
    3: ("Multi-condition: catalog_sales x item", query_3),
    4: ("Small baseline: store x item", query_4),
    5: ("Medium cross-product: customer x customer_address", query_5),
    6: ("Fact-to-fact theta: web_sales x store_sales(filtered)", query_6),
    7: ("SF100-safe: store x promotion date range", query_7),
    8: ("SF100-safe: date_dim(filtered) x store inequality", query_8),
    9: ("SF100-safe: web_page x catalog_page multi-condition", query_9),
    10: ("SF100-safe: item(filtered) x household_demographics", query_10),
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic Velox NLJ query plans for benchmarking."
    )
    parser.add_argument(
        "--output_dir",
        default="VeloxPlans/synthetic/nlj",
        help="Directory to write Q*.json files",
    )
    parser.add_argument(
        "--queries",
        default=",".join(str(q) for q in sorted(QUERIES)),
        help="Comma-separated query IDs to generate (default: all)",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    query_ids = [int(q.strip()) for q in args.queries.split(",")]
    for qid in query_ids:
        if qid not in QUERIES:
            print(f"WARNING: Unknown query ID {qid}, skipping")
            continue

        desc, gen_func = QUERIES[qid]
        reset_ids()
        plan = gen_func()

        path = os.path.join(args.output_dir, f"Q{qid}.json")
        with open(path, "w") as f:
            json.dump(plan, f, indent=2)
        print(f"Q{qid}: {desc} -> {path}")

    print(f"\nGenerated {len(query_ids)} plan(s) in {args.output_dir}")


if __name__ == "__main__":
    main()
