// The 24 tables of the TPC-DS schema, with every standard column. Types map as:
// identifier, integer -> int; decimal -> float; char, varchar -> string; date -> date.
// One entry per table: column names with a type letter (i int, f float, s string, d date).

import type { ColumnType } from "../../core/types.ts";
import type { ColumnDef } from "../../catalog/meta.ts";

const TEXT = `
call_center: cc_call_center_sk i, cc_call_center_id s, cc_rec_start_date d, cc_rec_end_date d,
  cc_closed_date_sk i, cc_open_date_sk i, cc_name s, cc_class s, cc_employees i, cc_sq_ft i,
  cc_hours s, cc_manager s, cc_mkt_id i, cc_mkt_class s, cc_mkt_desc s, cc_market_manager s,
  cc_division i, cc_division_name s, cc_company i, cc_company_name s, cc_street_number s,
  cc_street_name s, cc_street_type s, cc_suite_number s, cc_city s, cc_county s, cc_state s,
  cc_zip s, cc_country s, cc_gmt_offset f, cc_tax_percentage f
catalog_page: cp_catalog_page_sk i, cp_catalog_page_id s, cp_start_date_sk i, cp_end_date_sk i,
  cp_department s, cp_catalog_number i, cp_catalog_page_number i, cp_description s, cp_type s
catalog_returns: cr_returned_date_sk i, cr_returned_time_sk i, cr_item_sk i,
  cr_refunded_customer_sk i, cr_refunded_cdemo_sk i, cr_refunded_hdemo_sk i,
  cr_refunded_addr_sk i, cr_returning_customer_sk i, cr_returning_cdemo_sk i,
  cr_returning_hdemo_sk i, cr_returning_addr_sk i, cr_call_center_sk i, cr_catalog_page_sk i,
  cr_ship_mode_sk i, cr_warehouse_sk i, cr_reason_sk i, cr_order_number i,
  cr_return_quantity i, cr_return_amount f, cr_return_tax f, cr_return_amt_inc_tax f, cr_fee f,
  cr_return_ship_cost f, cr_refunded_cash f, cr_reversed_charge f, cr_store_credit f,
  cr_net_loss f
catalog_sales: cs_sold_date_sk i, cs_sold_time_sk i, cs_ship_date_sk i, cs_bill_customer_sk i,
  cs_bill_cdemo_sk i, cs_bill_hdemo_sk i, cs_bill_addr_sk i, cs_ship_customer_sk i,
  cs_ship_cdemo_sk i, cs_ship_hdemo_sk i, cs_ship_addr_sk i, cs_call_center_sk i,
  cs_catalog_page_sk i, cs_ship_mode_sk i, cs_warehouse_sk i, cs_item_sk i, cs_promo_sk i,
  cs_order_number i, cs_quantity i, cs_wholesale_cost f, cs_list_price f, cs_sales_price f,
  cs_ext_discount_amt f, cs_ext_sales_price f, cs_ext_wholesale_cost f, cs_ext_list_price f,
  cs_ext_tax f, cs_coupon_amt f, cs_ext_ship_cost f, cs_net_paid f, cs_net_paid_inc_tax f,
  cs_net_paid_inc_ship f, cs_net_paid_inc_ship_tax f, cs_net_profit f
customer: c_customer_sk i, c_customer_id s, c_current_cdemo_sk i, c_current_hdemo_sk i,
  c_current_addr_sk i, c_first_shipto_date_sk i, c_first_sales_date_sk i, c_salutation s,
  c_first_name s, c_last_name s, c_preferred_cust_flag s, c_birth_day i, c_birth_month i,
  c_birth_year i, c_birth_country s, c_login s, c_email_address s, c_last_review_date_sk i
customer_address: ca_address_sk i, ca_address_id s, ca_street_number s, ca_street_name s,
  ca_street_type s, ca_suite_number s, ca_city s, ca_county s, ca_state s, ca_zip s,
  ca_country s, ca_gmt_offset f, ca_location_type s
customer_demographics: cd_demo_sk i, cd_gender s, cd_marital_status s, cd_education_status s,
  cd_purchase_estimate i, cd_credit_rating s, cd_dep_count i, cd_dep_employed_count i,
  cd_dep_college_count i
date_dim: d_date_sk i, d_date_id s, d_date d, d_month_seq i, d_week_seq i, d_quarter_seq i,
  d_year i, d_dow i, d_moy i, d_dom i, d_qoy i, d_fy_year i, d_fy_quarter_seq i, d_fy_week_seq i,
  d_day_name s, d_quarter_name s, d_holiday s, d_weekend s, d_following_holiday s,
  d_first_dom i, d_last_dom i, d_same_day_ly i, d_same_day_lq i, d_current_day s,
  d_current_week s, d_current_month s, d_current_quarter s, d_current_year s
household_demographics: hd_demo_sk i, hd_income_band_sk i, hd_buy_potential s, hd_dep_count i,
  hd_vehicle_count i
income_band: ib_income_band_sk i, ib_lower_bound i, ib_upper_bound i
inventory: inv_date_sk i, inv_item_sk i, inv_warehouse_sk i, inv_quantity_on_hand i
item: i_item_sk i, i_item_id s, i_rec_start_date d, i_rec_end_date d, i_item_desc s,
  i_current_price f, i_wholesale_cost f, i_brand_id i, i_brand s, i_class_id i, i_class s,
  i_category_id i, i_category s, i_manufact_id i, i_manufact s, i_size s, i_formulation s,
  i_color s, i_units s, i_container s, i_manager_id i, i_product_name s
promotion: p_promo_sk i, p_promo_id s, p_start_date_sk i, p_end_date_sk i, p_item_sk i,
  p_cost f, p_response_target i, p_promo_name s, p_channel_dmail s, p_channel_email s,
  p_channel_catalog s, p_channel_tv s, p_channel_radio s, p_channel_press s,
  p_channel_event s, p_channel_demo s, p_channel_details s, p_purpose s, p_discount_active s
reason: r_reason_sk i, r_reason_id s, r_reason_desc s
ship_mode: sm_ship_mode_sk i, sm_ship_mode_id s, sm_type s, sm_code s, sm_carrier s,
  sm_contract s
store: s_store_sk i, s_store_id s, s_rec_start_date d, s_rec_end_date d, s_closed_date_sk i,
  s_store_name s, s_number_employees i, s_floor_space i, s_hours s, s_manager s,
  s_market_id i, s_geography_class s, s_market_desc s, s_market_manager s, s_division_id i,
  s_division_name s, s_company_id i, s_company_name s, s_street_number s, s_street_name s,
  s_street_type s, s_suite_number s, s_city s, s_county s, s_state s, s_zip s, s_country s,
  s_gmt_offset f, s_tax_precentage f
store_returns: sr_returned_date_sk i, sr_return_time_sk i, sr_item_sk i, sr_customer_sk i,
  sr_cdemo_sk i, sr_hdemo_sk i, sr_addr_sk i, sr_store_sk i, sr_reason_sk i,
  sr_ticket_number i, sr_return_quantity i, sr_return_amt f, sr_return_tax f,
  sr_return_amt_inc_tax f, sr_fee f, sr_return_ship_cost f, sr_refunded_cash f,
  sr_reversed_charge f, sr_store_credit f, sr_net_loss f
store_sales: ss_sold_date_sk i, ss_sold_time_sk i, ss_item_sk i, ss_customer_sk i,
  ss_cdemo_sk i, ss_hdemo_sk i, ss_addr_sk i, ss_store_sk i, ss_promo_sk i,
  ss_ticket_number i, ss_quantity i, ss_wholesale_cost f, ss_list_price f, ss_sales_price f,
  ss_ext_discount_amt f, ss_ext_sales_price f, ss_ext_wholesale_cost f, ss_ext_list_price f,
  ss_ext_tax f, ss_coupon_amt f, ss_net_paid f, ss_net_paid_inc_tax f, ss_net_profit f
time_dim: t_time_sk i, t_time_id s, t_time i, t_hour i, t_minute i, t_second i, t_am_pm s,
  t_shift s, t_sub_shift s, t_meal_time s
warehouse: w_warehouse_sk i, w_warehouse_id s, w_warehouse_name s, w_warehouse_sq_ft i,
  w_street_number s, w_street_name s, w_street_type s, w_suite_number s, w_city s,
  w_county s, w_state s, w_zip s, w_country s, w_gmt_offset f
web_page: wp_web_page_sk i, wp_web_page_id s, wp_rec_start_date d, wp_rec_end_date d,
  wp_creation_date_sk i, wp_access_date_sk i, wp_autogen_flag s, wp_customer_sk i, wp_url s,
  wp_type s, wp_char_count i, wp_link_count i, wp_image_count i, wp_max_ad_count i
web_returns: wr_returned_date_sk i, wr_returned_time_sk i, wr_item_sk i,
  wr_refunded_customer_sk i, wr_refunded_cdemo_sk i, wr_refunded_hdemo_sk i,
  wr_refunded_addr_sk i, wr_returning_customer_sk i, wr_returning_cdemo_sk i,
  wr_returning_hdemo_sk i, wr_returning_addr_sk i, wr_web_page_sk i, wr_reason_sk i,
  wr_order_number i, wr_return_quantity i, wr_return_amt f, wr_return_tax f,
  wr_return_amt_inc_tax f, wr_fee f, wr_return_ship_cost f, wr_refunded_cash f,
  wr_reversed_charge f, wr_account_credit f, wr_net_loss f
web_sales: ws_sold_date_sk i, ws_sold_time_sk i, ws_ship_date_sk i, ws_item_sk i,
  ws_bill_customer_sk i, ws_bill_cdemo_sk i, ws_bill_hdemo_sk i, ws_bill_addr_sk i,
  ws_ship_customer_sk i, ws_ship_cdemo_sk i, ws_ship_hdemo_sk i, ws_ship_addr_sk i,
  ws_web_page_sk i, ws_web_site_sk i, ws_ship_mode_sk i, ws_warehouse_sk i, ws_promo_sk i,
  ws_order_number i, ws_quantity i, ws_wholesale_cost f, ws_list_price f, ws_sales_price f,
  ws_ext_discount_amt f, ws_ext_sales_price f, ws_ext_wholesale_cost f, ws_ext_list_price f,
  ws_ext_tax f, ws_coupon_amt f, ws_ext_ship_cost f, ws_net_paid f, ws_net_paid_inc_tax f,
  ws_net_paid_inc_ship f, ws_net_paid_inc_ship_tax f, ws_net_profit f
web_site: web_site_sk i, web_site_id s, web_rec_start_date d, web_rec_end_date d, web_name s,
  web_open_date_sk i, web_close_date_sk i, web_class s, web_manager s, web_mkt_id i,
  web_mkt_class s, web_mkt_desc s, web_market_manager s, web_company_id i,
  web_company_name s, web_street_number s, web_street_name s, web_street_type s,
  web_suite_number s, web_city s, web_county s, web_state s, web_zip s, web_country s,
  web_gmt_offset f, web_tax_percentage f
`;

const TYPES: Record<string, ColumnType> = { i: "int", f: "float", s: "string", d: "date" };

function parse(text: string): Record<string, ColumnDef[]> {
  const out: Record<string, ColumnDef[]> = {};
  let body: ColumnDef[] = [];
  for (const line of text.trim().split("\n")) {
    let rest = line;
    if (!line.startsWith(" ")) {
      const [name, r] = line.split(/:(.*)/s) as [string, string];
      body = out[name] = [];
      rest = r;
    }
    for (const part of rest.split(",")) {
      const p = part.trim();
      if (!p) continue;
      const [col, t] = p.split(/\s+/) as [string, string];
      body.push({ name: col, type: TYPES[t]! });
    }
  }
  return out;
}

export const SCHEMA = parse(TEXT);
export const TABLES = Object.keys(SCHEMA);
