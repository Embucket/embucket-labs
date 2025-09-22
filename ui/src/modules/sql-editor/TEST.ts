export const QUERY_RECORD_MOCK = {
  id: 8242581797074,
  worksheetId: 1757415810829,
  query:
    "with cross_items as (\n  select\n    i_item_sk ss_item_sk\n  from item_7364a263_spark, (\n      select\n        iss.i_brand_id brand_id,\n        iss.i_class_id class_id,\n        iss.i_category_id category_id\n      from\n        store_sales_7364a263_spark, item_7364a263_spark iss, date_dim_7364a263_spark d1\n      where\n        ss_item_sk = iss.i_item_sk\n          and ss_sold_date_sk = d1.d_date_sk\n          and d1.d_year between 1998 AND 1998 + 2\n      intersect\n      select\n        ics.i_brand_id,\n        ics.i_class_id,\n        ics.i_category_id\n      from\n        catalog_sales_7364a263_spark, item_7364a263_spark ics, date_dim_7364a263_spark d2\n      where\n        cs_item_sk = ics.i_item_sk\n          and cs_sold_date_sk = d2.d_date_sk\n          and d2.d_year between 1998 AND 1998 + 2\n      intersect\n      select\n        iws.i_brand_id,\n        iws.i_class_id,\n        iws.i_category_id\n      from\n        web_sales_7364a263_spark, item_7364a263_spark iws, date_dim_7364a263_spark d3\n      where\n        ws_item_sk = iws.i_item_sk\n          and ws_sold_date_sk = d3.d_date_sk\n          and d3.d_year between 1998 AND 1998 + 2) x\n      where\n        i_brand_id = brand_id\n          and i_class_id = class_id\n          and i_category_id = category_id),\navg_sales as (\n  select\n    avg(quantity*list_price) average_sales\n  from (\n      select\n        ss_quantity quantity,\n        ss_list_price list_price\n      from\n        store_sales_7364a263_spark, date_dim_7364a263_spark\n      where\n        ss_sold_date_sk = d_date_sk\n          and d_year between 1998 and 1998 + 2\n      union all\n      select\n        cs_quantity quantity,\n        cs_list_price list_price\n      from\n        catalog_sales_7364a263_spark, date_dim_7364a263_spark\n      where\n        cs_sold_date_sk = d_date_sk\n          and d_year between 1998 and 1998 + 2\n      union all\n      select\n        ws_quantity quantity,\n        ws_list_price list_price\n      from\n        web_sales_7364a263_spark, date_dim_7364a263_spark\n      where\n        ws_sold_date_sk = d_date_sk\n          and d_year between 1998 and 1998 + 2) x)\nselect\n  *\nfrom (\n    select\n      'store' channel,\n      i_brand_id,\n      i_class_id,\n      i_category_id,\n      sum(ss_quantity * ss_list_price) sales,\n      count(*) number_sales\n    from\n      store_sales_7364a263_spark, item_7364a263_spark, date_dim_7364a263_spark\n    where\n      ss_item_sk in (select ss_item_sk from cross_items)\n        and ss_item_sk = i_item_sk\n        and ss_sold_date_sk = d_date_sk\n        and d_week_seq = (\n            select d_week_seq\n            from date_dim_7364a263_spark\n            where d_year = 1998 + 1\n              and d_moy = 12\n              and d_dom = 16)\n    group by\n      i_brand_id,\n      i_class_id,\n      i_category_id\n    having\n      sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) this_year,\n  (\n    select\n      'store' channel,\n      i_brand_id,\n      i_class_id,\n      i_category_id,\n      sum(ss_quantity * ss_list_price) sales,\n      count(*) number_sales\n    from\n      store_sales_7364a263_spark, item_7364a263_spark, date_dim_7364a263_spark\n    where\n      ss_item_sk in (select ss_item_sk from cross_items)\n        and ss_item_sk = i_item_sk\n        and ss_sold_date_sk = d_date_sk\n        and d_week_seq = (\n            select d_week_seq\n            from date_dim_7364a263_spark\n            where d_year = 1998\n              and d_moy = 12\n              and d_dom = 16)\n    group by\n      i_brand_id,\n      i_class_id,\n      i_category_id\n    having\n      sum(ss_quantity * ss_list_price) > (select average_sales from avg_sales)) last_year\nwhere\n  this_year.i_brand_id = last_year.i_brand_id\n    and this_year.i_class_id = last_year.i_class_id\n    and this_year.i_category_id = last_year.i_category_id\norder by\n  this_year.channel,\n  this_year.i_brand_id,\n  this_year.i_class_id,\n  this_year.i_category_id",
  startTime: '2025-09-09T11:43:22.925829Z',
  endTime: '2025-09-09T11:43:25.486571Z',
  durationMs: 2560,
  resultCount: 228,
  result: {
    columns: [
      {
        name: 'channel',
        type: 'text',
      },
      {
        name: 'i_brand_id',
        type: 'fixed',
      },
      {
        name: 'i_class_id',
        type: 'fixed',
      },
      {
        name: 'i_category_id',
        type: 'fixed',
      },
      {
        name: 'sales',
        type: 'fixed',
      },
      {
        name: 'number_sales',
        type: 'fixed',
      },
      {
        name: 'channel',
        type: 'text',
      },
      {
        name: 'i_brand_id',
        type: 'fixed',
      },
      {
        name: 'i_class_id',
        type: 'fixed',
      },
      {
        name: 'i_category_id',
        type: 'fixed',
      },
      {
        name: 'sales',
        type: 'fixed',
      },
      {
        name: 'number_sales',
        type: 'fixed',
      },
    ],
    rows: [['store', 1001001, 1, 1, 127238.15, 34, 'store', 1001001, 1, 1, 127238.15, 34]],
  },
  status: 'successful',
  error: '',
};
