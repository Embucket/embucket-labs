from .tpcds_table_names import parametrize_tpcds_statements

_TPCDS_QUERIES_RAW = [
    (
        "query01",
        """


        with customer_total_return as
                                                                     (select sr_customer_sk as ctr_customer_sk
                                                                           ,sr_store_sk as ctr_store_sk
                                                                           ,sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return
                                                                      from {STORE_RETURNS_TABLE}
                                                                         ,{DATE_DIM_TABLE}
                                                                      where sr_returned_date_sk = d_date_sk
                                                                        and d_year =1999
                                                                      group by sr_customer_sk
                                                                             ,sr_store_sk)
select  c_customer_id
from customer_total_return ctr1
   ,{STORE_TABLE}
   ,{CUSTOMER_TABLE}
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
                               from customer_total_return ctr2
                               where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = 'NM'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
    limit 100
                        """
    ),
    (
        "query02",
        """
        with wscs as
                                                                     (select sold_date_sk
                                                                           ,sales_price
                                                                      from  (select ws_sold_date_sk sold_date_sk
                                                                                  ,ws_ext_sales_price sales_price
                                                                             from {WEB_SALES_TABLE}
                                                                             union all
                                                                             select cs_sold_date_sk sold_date_sk
                                                                                  ,cs_ext_sales_price sales_price
                                                                             from {CATALOG_SALES_TABLE}) x ),
                                                                 wswscs as
                                                                     (select d_week_seq,
                                                                             sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
                                                                             sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
                                                                             sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
                                                                             sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
                                                                             sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
                                                                             sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
                                                                             sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
                                                                      from wscs
                                                                         ,{DATE_DIM_TABLE}
                                                                      where d_date_sk = sold_date_sk
                                                                      group by d_week_seq)
select d_week_seq1
     ,round(sun_sales1/sun_sales2,2)
     ,round(mon_sales1/mon_sales2,2)
     ,round(tue_sales1/tue_sales2,2)
     ,round(wed_sales1/wed_sales2,2)
     ,round(thu_sales1/thu_sales2,2)
     ,round(fri_sales1/fri_sales2,2)
     ,round(sat_sales1/sat_sales2,2)
from
    (select wswscs.d_week_seq d_week_seq1
          ,sun_sales sun_sales1
          ,mon_sales mon_sales1
          ,tue_sales tue_sales1
          ,wed_sales wed_sales1
          ,thu_sales thu_sales1
          ,fri_sales fri_sales1
          ,sat_sales sat_sales1
     from wswscs,{DATE_DIM_TABLE}
     where {DATE_DIM_TABLE}.d_week_seq = wswscs.d_week_seq and
             d_year = 1999) y,
    (select wswscs.d_week_seq d_week_seq2
          ,sun_sales sun_sales2
          ,mon_sales mon_sales2
          ,tue_sales tue_sales2
          ,wed_sales wed_sales2
          ,thu_sales thu_sales2
          ,fri_sales fri_sales2
          ,sat_sales sat_sales2
     from wswscs
        ,{DATE_DIM_TABLE}
     where {DATE_DIM_TABLE}.d_week_seq = wswscs.d_week_seq and
             d_year = 1999+1) z
where d_week_seq1=d_week_seq2-53
order by d_week_seq1
                        """
    ),
    (
        "query03",
        """


        select dt.d_year
     ,{ITEM_TABLE}.i_brand_id brand_id
     ,{ITEM_TABLE}.i_brand brand
     ,sum(ss_ext_discount_amt) sum_agg
from  {DATE_DIM_TABLE} dt
   ,{STORE_SALES_TABLE}
   ,{ITEM_TABLE}
where dt.d_date_sk = {STORE_SALES_TABLE}.ss_sold_date_sk
  and {STORE_SALES_TABLE}.ss_item_sk = {ITEM_TABLE}.i_item_sk
  and {ITEM_TABLE}.i_manufact_id = 540
  and dt.d_moy=12
group by dt.d_year
       ,{ITEM_TABLE}.i_brand
       ,{ITEM_TABLE}.i_brand_id
order by dt.d_year
       ,sum_agg desc
       ,brand_id
    limit 100
                        """
    ),
    (
        "query04",
        """


        with year_total as (
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,c_preferred_cust_flag customer_preferred_cust_flag
         ,c_birth_country customer_birth_country
         ,c_login customer_login
         ,c_email_address customer_email_address
         ,d_year dyear
         ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
         ,'s' sale_type
    from {CUSTOMER_TABLE}
       ,{STORE_SALES_TABLE}
       ,{DATE_DIM_TABLE}
    where c_customer_sk = ss_customer_sk
      and ss_sold_date_sk = d_date_sk
    group by c_customer_id
           ,c_first_name
           ,c_last_name
           ,c_preferred_cust_flag
           ,c_birth_country
           ,c_login
           ,c_email_address
           ,d_year
    union all
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,c_preferred_cust_flag customer_preferred_cust_flag
         ,c_birth_country customer_birth_country
         ,c_login customer_login
         ,c_email_address customer_email_address
         ,d_year dyear
         ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
         ,'c' sale_type
    from {CUSTOMER_TABLE}
       ,{CATALOG_SALES_TABLE}
       ,{DATE_DIM_TABLE}
    where c_customer_sk = cs_bill_customer_sk
      and cs_sold_date_sk = d_date_sk
    group by c_customer_id
           ,c_first_name
           ,c_last_name
           ,c_preferred_cust_flag
           ,c_birth_country
           ,c_login
           ,c_email_address
           ,d_year
    union all
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,c_preferred_cust_flag customer_preferred_cust_flag
         ,c_birth_country customer_birth_country
         ,c_login customer_login
         ,c_email_address customer_email_address
         ,d_year dyear
         ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
         ,'w' sale_type
    from {CUSTOMER_TABLE}
       ,{WEB_SALES_TABLE}
       ,{DATE_DIM_TABLE}
    where c_customer_sk = ws_bill_customer_sk
      and ws_sold_date_sk = d_date_sk
    group by c_customer_id
           ,c_first_name
           ,c_last_name
           ,c_preferred_cust_flag
           ,c_birth_country
           ,c_login
           ,c_email_address
           ,d_year
)
select
    t_s_secyear.customer_id
     ,t_s_secyear.customer_first_name
     ,t_s_secyear.customer_last_name
     ,t_s_secyear.customer_login
from year_total t_s_firstyear
   ,year_total t_s_secyear
   ,year_total t_c_firstyear
   ,year_total t_c_secyear
   ,year_total t_w_firstyear
   ,year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_c_secyear.customer_id
  and t_s_firstyear.customer_id = t_c_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_c_firstyear.sale_type = 'c'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_c_secyear.sale_type = 'c'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear =  1999
  and t_s_secyear.dyear = 1999+1
  and t_c_firstyear.dyear =  1999
  and t_c_secyear.dyear =  1999+1
  and t_w_firstyear.dyear = 1999
  and t_w_secyear.dyear = 1999+1
  and t_s_firstyear.year_total > 0
  and t_c_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
    > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
  and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
    > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
order by t_s_secyear.customer_id
       ,t_s_secyear.customer_first_name
       ,t_s_secyear.customer_last_name
       ,t_s_secyear.customer_login
    limit 100
                        """
    ),
    (
        "query05",
        """


        with ssr as
                                                                     (select s_store_id,
                                                                             sum(sales_price) as sales,
                                                                             sum(profit) as profit,
                                                                             sum(return_amt) as returns,
                                                                             sum(net_loss) as profit_loss
                                                                      from
                                                                          ( select  ss_store_sk as store_sk,
                                                                                    ss_sold_date_sk  as date_sk,
                                                                                    ss_ext_sales_price as sales_price,
                                                                                    ss_net_profit as profit,
                                                                                    cast(0 as decimal(7,2)) as return_amt,
                                                                                    cast(0 as decimal(7,2)) as net_loss
                                                                            from {STORE_SALES_TABLE}
                                                                            union all
                                                                            select sr_store_sk as store_sk,
                                                                                   sr_returned_date_sk as date_sk,
                                                                                   cast(0 as decimal(7,2)) as sales_price,
                                                                                   cast(0 as decimal(7,2)) as profit,
                                                                                   sr_return_amt as return_amt,
                                                                                   sr_net_loss as net_loss
                                                                            from {STORE_RETURNS_TABLE}
                                                                          ) salesreturns,
                                                                          {DATE_DIM_TABLE},
                                                                          {STORE_TABLE}
                                                                      where date_sk = d_date_sk
                                                                        and d_date between cast('2002-08-09' as date)
                                                                          and dateadd(day, 14, cast('2002-08-09' as date))
                                                                        and store_sk = s_store_sk
                                                                      group by s_store_id)
        ,
                                                                 csr as
                                                                     (select cp_catalog_page_id,
                                                                             sum(sales_price) as sales,
                                                                             sum(profit) as profit,
                                                                             sum(return_amt) as returns,
                                                                             sum(net_loss) as profit_loss
                                                                      from
                                                                          ( select  cs_catalog_page_sk as page_sk,
                                                                                    cs_sold_date_sk  as date_sk,
                                                                                    cs_ext_sales_price as sales_price,
                                                                                    cs_net_profit as profit,
                                                                                    cast(0 as decimal(7,2)) as return_amt,
                                                                                    cast(0 as decimal(7,2)) as net_loss
                                                                            from {CATALOG_SALES_TABLE}
                                                                            union all
                                                                            select cr_catalog_page_sk as page_sk,
                                                                                   cr_returned_date_sk as date_sk,
                                                                                   cast(0 as decimal(7,2)) as sales_price,
                                                                                   cast(0 as decimal(7,2)) as profit,
                                                                                   cr_return_amount as return_amt,
                                                                                   cr_net_loss as net_loss
                                                                            from {CATALOG_RETURNS_TABLE}
                                                                          ) salesreturns,
                                                                          {DATE_DIM_TABLE},
                                                                          {CATALOG_PAGE_TABLE}
                                                                      where date_sk = d_date_sk
                                                                        and d_date between cast('2002-08-09' as date)
                                                                          and dateadd(day, 14, cast('2002-08-09' as date))
                                                                        and page_sk = cp_catalog_page_sk
                                                                      group by cp_catalog_page_id)
        ,
                                                                 wsr as
                                                                     (select web_site_id,
                                                                             sum(sales_price) as sales,
                                                                             sum(profit) as profit,
                                                                             sum(return_amt) as returns,
                                                                             sum(net_loss) as profit_loss
                                                                      from
                                                                          ( select  ws_web_site_sk as wsr_web_site_sk,
                                                                                    ws_sold_date_sk  as date_sk,
                                                                                    ws_ext_sales_price as sales_price,
                                                                                    ws_net_profit as profit,
                                                                                    cast(0 as decimal(7,2)) as return_amt,
                                                                                    cast(0 as decimal(7,2)) as net_loss
                                                                            from {WEB_SALES_TABLE}
                                                                            union all
                                                                            select ws_web_site_sk as wsr_web_site_sk,
                                                                                   wr_returned_date_sk as date_sk,
                                                                                   cast(0 as decimal(7,2)) as sales_price,
                                                                                   cast(0 as decimal(7,2)) as profit,
                                                                                   wr_return_amt as return_amt,
                                                                                   wr_net_loss as net_loss
                                                                            from {WEB_RETURNS_TABLE} left outer join {WEB_SALES_TABLE} on
                                                                                ( wr_item_sk = ws_item_sk
                                                                                    and wr_order_number = ws_order_number)
                                                                          ) salesreturns,
                                                                          {DATE_DIM_TABLE},
                                                                          {WEB_SITE_TABLE}
                                                                      where date_sk = d_date_sk
                                                                        and d_date between cast('2002-08-09' as date)
                                                                          and dateadd(day, 14, cast('2002-08-09' as date))
                                                                        and wsr_web_site_sk = web_site_sk
                                                                      group by web_site_id)
select  channel
     , id
     , sum(sales) as sales
     , sum(returns) as returns
     , sum(profit) as profit
from
    (select '{STORE_TABLE} channel' as channel
          , '{STORE_TABLE}' || s_store_id as id
          , sales
          , returns
          , (profit - profit_loss) as profit
     from   ssr
     union all
     select 'catalog channel' as channel
          , '{CATALOG_PAGE_TABLE}' || cp_catalog_page_id as id
          , sales
          , returns
          , (profit - profit_loss) as profit
     from  csr
     union all
     select 'web channel' as channel
          , '{WEB_SITE_TABLE}' || web_site_id as id
          , sales
          , returns
          , (profit - profit_loss) as profit
     from   wsr
    ) x
group by rollup (channel, id)
order by channel
       ,id
    limit 100
                        """
    ),
    (
        "query06",
        """


        select a.ca_state state, count(*) cnt
from {CUSTOMER_ADDRESS_TABLE} a
   ,{CUSTOMER_TABLE} c
   ,{STORE_SALES_TABLE} s
   ,{DATE_DIM_TABLE} d
   ,{ITEM_TABLE} i
where       a.ca_address_sk = c.c_current_addr_sk
  and c.c_customer_sk = s.ss_customer_sk
  and s.ss_sold_date_sk = d.d_date_sk
  and s.ss_item_sk = i.i_item_sk
  and d.d_month_seq =
      (select distinct (d_month_seq)
       from {DATE_DIM_TABLE}
       where d_year = 2002
         and d_moy = 7 )
  and i.i_current_price > 1.2 *
                          (select avg(j.i_current_price)
                           from {ITEM_TABLE} j
                           where j.i_category = i.i_category)
group by a.ca_state
having count(*) >= 10
order by cnt, a.ca_state
    limit 100
                        """
    ),
    (
        "query07",
        """


        select i_item_id,
                                                                   avg(ss_quantity) agg1,
                                                                   avg(ss_list_price) agg2,
                                                                   avg(ss_coupon_amt) agg3,
                                                                   avg(ss_sales_price) agg4
from {STORE_SALES_TABLE}, {CUSTOMER_DEMOGRAPHICS_TABLE}, {DATE_DIM_TABLE}, {ITEM_TABLE}, {PROMOTION_TABLE}
where ss_sold_date_sk = d_date_sk and
        ss_item_sk = i_item_sk and
        ss_cdemo_sk = cd_demo_sk and
        ss_promo_sk = p_promo_sk and
        cd_gender = 'M' and
        cd_marital_status = 'M' and
        cd_education_status = '4 yr Degree' and
    (p_channel_email = 'N' or p_channel_event = 'N') and
        d_year = 2001
group by i_item_id
order by i_item_id
    limit 100
                        """
    ),
    (
        "query08",
        """


        select s_store_name
     ,sum(ss_net_profit)
from {STORE_SALES_TABLE}
   ,{DATE_DIM_TABLE}
   ,{STORE_TABLE},
    (select ca_zip
     from (
              SELECT substr(ca_zip,1,5) ca_zip
              FROM {CUSTOMER_ADDRESS_TABLE}
              WHERE substr(ca_zip,1,5) IN (
                                           '10338','56623','51423','26456','19500','65832',
                                           '17178','68879','49935','49849','93956',
                                           '71765','45100','50587','68389','41899',
                                           '98316','56217','94686','59350','32857',
                                           '14925','31266','37817','27519','20787',
                                           '26967','49045','39397','32010','23144',
                                           '53580','15491','74151','18442','51916',
                                           '17730','22824','28290','21657','45460',
                                           '39386','21133','35017','19894','21759',
                                           '79293','86733','76777','41688','13810',
                                           '49053','17992','13395','19869','40785',
                                           '63897','65049','27388','94701','41482',
                                           '97923','23951','88284','61718','94317',
                                           '72294','63544','31306','41242','28830',
                                           '75535','86189','88177','16147','12902',
                                           '48271','54036','20936','27802','96741',
                                           '70286','75710','16034','90285','22058',
                                           '52590','40584','62441','64039','68999',
                                           '64327','33844','52497','88495','25989',
                                           '67814','13767','83194','99395','35524',
                                           '89640','48834','51875','71073','25383',
                                           '19129','57805','47962','61905','19557',
                                           '74159','98032','13917','50936','47993',
                                           '41606','17592','11470','28216','19732',
                                           '97958','60997','85688','96863','16605',
                                           '10898','31340','71340','72902','98949',
                                           '74440','53057','30323','76166','27195',
                                           '11204','32771','38189','83221','22295',
                                           '15325','20844','65549','69207','71903',
                                           '63929','56922','25733','75482','14986',
                                           '79223','73692','98769','70275','33793',
                                           '13057','30142','95737','30072','32097',
                                           '25845','50282','19289','92221','59533',
                                           '37375','29706','48186','22385','55809',
                                           '17416','10592','55385','71829','91975',
                                           '73557','38036','10448','95252','51386',
                                           '14190','15247','39907','79438','78053',
                                           '66623','27720','84139','74147','58637',
                                           '11434','36573','10081','53536','41724',
                                           '97898','36752','50384','87352','35696',
                                           '69486','50026','27837','42592','58865',
                                           '80523','53682','65423','77611','98529',
                                           '13909','13727','52190','36152','48355',
                                           '62496','16527','18143','98830','75198',
                                           '73043','64043','63042','67797','50656',
                                           '27700','60687','57905','94404','15733',
                                           '80809','74562','84493','67977','11213',
                                           '19125','84496','16435','97510','46040',
                                           '33968','20256','42332','16480','54277',
                                           '82819','93799','69101','57689','42821',
                                           '68073','49342','46915','25825','92332',
                                           '20219','96577','49463','19221','35814',
                                           '64783','97303','52061','24357','58167',
                                           '56286','64474','99847','53626','39703',
                                           '24880','24365','50652','29611','90638',
                                           '59246','27171','30483','11708','38630',
                                           '81914','48269','11720','88662','68844',
                                           '54838','93795','38102','33481','97546',
                                           '49306','97216','49032','14270','72418',
                                           '32540','53208','15588','29990','10407',
                                           '92334','48543','51495','77996','53686',
                                           '14827','30978','30482','86296','48869',
                                           '59600','29495','24775','34645','19763',
                                           '98602','20456','10468','13887','65714',
                                           '74740','37096','96240','44111','54109',
                                           '62693','87874','64295','62027','86027',
                                           '54341','68582','67809','44159','97913',
                                           '79150','38974','64754','73946','20840',
                                           '16138','58939','20428','19890','70842',
                                           '78648','55576','37267','40470','12957',
                                           '57553','53593','34067','22555','79719',
                                           '25809','28496','11083','87624','83622',
                                           '84898','28678','14297','79461','22910',
                                           '87129','49941','64817','93905','39721',
                                           '81837','18753','86432','67821','66080',
                                           '28246','13466','16363','56950','35446',
                                           '58326','11760','33962','28399','45848',
                                           '52560','66894','15169','20988','85925',
                                           '38582','34825','94227','56758','24801',
                                           '14128','14012','35824','49784')
              intersect
              select ca_zip
              from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
                    FROM {CUSTOMER_ADDRESS_TABLE}, {CUSTOMER_TABLE}
                    WHERE ca_address_sk = c_current_addr_sk and
                            c_preferred_cust_flag='Y'
                    group by ca_zip
                    having count(*) > 10)A1)A2) V1
where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_qoy = 1 and d_year = 2002
  and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))
group by s_store_name
order by s_store_name
    limit 100
                        """
    ),
    (
        "query09",
        """


        select case when (select count(*)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 1 and 20) > 177560856
                                                                            then (select avg(ss_ext_discount_amt)
                                                                                  from {STORE_SALES_TABLE}
                                                                                  where ss_quantity between 1 and 20)
                                                                        else (select avg(ss_net_paid_inc_tax)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 1 and 20) end bucket1 ,
                                                                   case when (select count(*)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 21 and 40) > 165030289
                                                                            then (select avg(ss_ext_discount_amt)
                                                                                  from {STORE_SALES_TABLE}
                                                                                  where ss_quantity between 21 and 40)
                                                                        else (select avg(ss_net_paid_inc_tax)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 21 and 40) end bucket2,
                                                                   case when (select count(*)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 41 and 60) > 357290662
                                                                            then (select avg(ss_ext_discount_amt)
                                                                                  from {STORE_SALES_TABLE}
                                                                                  where ss_quantity between 41 and 60)
                                                                        else (select avg(ss_net_paid_inc_tax)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 41 and 60) end bucket3,
                                                                   case when (select count(*)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 61 and 80) > 205197751
                                                                            then (select avg(ss_ext_discount_amt)
                                                                                  from {STORE_SALES_TABLE}
                                                                                  where ss_quantity between 61 and 80)
                                                                        else (select avg(ss_net_paid_inc_tax)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 61 and 80) end bucket4,
                                                                   case when (select count(*)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 81 and 100) > 80111186
                                                                            then (select avg(ss_ext_discount_amt)
                                                                                  from {STORE_SALES_TABLE}
                                                                                  where ss_quantity between 81 and 100)
                                                                        else (select avg(ss_net_paid_inc_tax)
                                                                              from {STORE_SALES_TABLE}
                                                                              where ss_quantity between 81 and 100) end bucket5
from {REASON_TABLE}
where r_reason_sk = 1
                        """
    ),
    (
        "query10",
        """


        select cd_gender,
    cd_marital_status,
    cd_education_status,
    count(*) cnt1,
    cd_purchase_estimate,
    count(*) cnt2,
    cd_credit_rating,
    count(*) cnt3,
    cd_dep_count,
    count(*) cnt4,
    cd_dep_employed_count,
    count(*) cnt5,
    cd_dep_college_count,
    count(*) cnt6
from
    {CUSTOMER_TABLE} c,{CUSTOMER_ADDRESS_TABLE} ca,{CUSTOMER_DEMOGRAPHICS_TABLE}
where
        c.c_current_addr_sk = ca.ca_address_sk and
        ca_county in ('Polk County','Denton County','Union County','Swisher County','Cherokee County') and
        cd_demo_sk = c.c_current_cdemo_sk and
    exists (select *
            from {STORE_SALES_TABLE},{DATE_DIM_TABLE}
            where c.c_customer_sk = ss_customer_sk and
                    ss_sold_date_sk = d_date_sk and
                    d_year = 2002 and
                d_moy between 3 and 3+3) and
    (exists (select *
             from {WEB_SALES_TABLE},{DATE_DIM_TABLE}
             where c.c_customer_sk = ws_bill_customer_sk and
                     ws_sold_date_sk = d_date_sk and
                     d_year = 2002 and
                 d_moy between 3 ANd 3+3) or
     exists (select *
             from {CATALOG_SALES_TABLE},{DATE_DIM_TABLE}
             where c.c_customer_sk = cs_ship_customer_sk and
                     cs_sold_date_sk = d_date_sk and
                     d_year = 2002 and
                 d_moy between 3 and 3+3))
group by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
order by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
    limit 100
                        """
    ),
    (
        "query11",
        """


        with year_total as (
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,c_preferred_cust_flag customer_preferred_cust_flag
         ,c_birth_country customer_birth_country
         ,c_login customer_login
         ,c_email_address customer_email_address
         ,d_year dyear
         ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
         ,'s' sale_type
    from {CUSTOMER_TABLE}
       ,{STORE_SALES_TABLE}
       ,{DATE_DIM_TABLE}
    where c_customer_sk = ss_customer_sk
      and ss_sold_date_sk = d_date_sk
    group by c_customer_id
           ,c_first_name
           ,c_last_name
           ,c_preferred_cust_flag
           ,c_birth_country
           ,c_login
           ,c_email_address
           ,d_year
    union all
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,c_preferred_cust_flag customer_preferred_cust_flag
         ,c_birth_country customer_birth_country
         ,c_login customer_login
         ,c_email_address customer_email_address
         ,d_year dyear
         ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
         ,'w' sale_type
    from {CUSTOMER_TABLE}
       ,{WEB_SALES_TABLE}
       ,{DATE_DIM_TABLE}
    where c_customer_sk = ws_bill_customer_sk
      and ws_sold_date_sk = d_date_sk
    group by c_customer_id
           ,c_first_name
           ,c_last_name
           ,c_preferred_cust_flag
           ,c_birth_country
           ,c_login
           ,c_email_address
           ,d_year
)
select
    t_s_secyear.customer_id
     ,t_s_secyear.customer_first_name
     ,t_s_secyear.customer_last_name
     ,t_s_secyear.customer_login
from year_total t_s_firstyear
   ,year_total t_s_secyear
   ,year_total t_w_firstyear
   ,year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 1999
  and t_s_secyear.dyear = 1999+1
  and t_w_firstyear.dyear = 1999
  and t_w_secyear.dyear = 1999+1
  and t_s_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end
    > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end
order by t_s_secyear.customer_id
       ,t_s_secyear.customer_first_name
       ,t_s_secyear.customer_last_name
       ,t_s_secyear.customer_login
    limit 100
                        """
    ),
    (
        "query12",
        """


        select i_item_id
     ,i_item_desc
     ,i_category
     ,i_class
     ,i_current_price
     ,sum(ws_ext_sales_price) as itemrevenue
     ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
    (partition by i_class) as revenueratio
from
    {WEB_SALES_TABLE}
   ,{ITEM_TABLE}
   ,{DATE_DIM_TABLE}
where
        ws_item_sk = i_item_sk
  and i_category in ('Shoes', 'Children', 'Electronics')
  and ws_sold_date_sk = d_date_sk
  and d_date between cast('2002-04-12' as date)
    and dateadd(day,30,to_date('2002-04-12'))
group by
    i_item_id
       ,i_item_desc
       ,i_category
       ,i_class
       ,i_current_price
order by
    i_category
       ,i_class
       ,i_item_id
       ,i_item_desc
       ,revenueratio
    limit 100
                        """
    ),
    (
        "query13",
        """


        select avg(ss_quantity)
     ,avg(ss_ext_sales_price)
     ,avg(ss_ext_wholesale_cost)
     ,sum(ss_ext_wholesale_cost)
from {STORE_SALES_TABLE}
   ,{STORE_TABLE}
   ,{CUSTOMER_DEMOGRAPHICS_TABLE}
   ,{HOUSEHOLD_DEMOGRAPHICS_TABLE}
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{DATE_DIM_TABLE}
where s_store_sk = ss_store_sk
  and  ss_sold_date_sk = d_date_sk and d_year = 2001
  and((ss_hdemo_sk=hd_demo_sk
    and cd_demo_sk = ss_cdemo_sk
    and cd_marital_status = 'D'
    and cd_education_status = '2 yr Degree'
    and ss_sales_price between 100.00 and 150.00
    and hd_dep_count = 3
          )or
      (ss_hdemo_sk=hd_demo_sk
          and cd_demo_sk = ss_cdemo_sk
          and cd_marital_status = 'U'
          and cd_education_status = 'Primary'
          and ss_sales_price between 50.00 and 100.00
          and hd_dep_count = 1
          ) or
      (ss_hdemo_sk=hd_demo_sk
          and cd_demo_sk = ss_cdemo_sk
          and cd_marital_status = 'W'
          and cd_education_status = 'Secondary'
          and ss_sales_price between 150.00 and 200.00
          and hd_dep_count = 1
          ))
  and((ss_addr_sk = ca_address_sk
    and ca_country = 'United States'
    and ca_state in ('MI', 'MN', 'NC')
    and ss_net_profit between 100 and 200
          ) or
      (ss_addr_sk = ca_address_sk
          and ca_country = 'United States'
          and ca_state in ('GA', 'OH', 'MT')
          and ss_net_profit between 150 and 300
          ) or
      (ss_addr_sk = ca_address_sk
          and ca_country = 'United States'
          and ca_state in ('MS', 'AK', 'OK')
          and ss_net_profit between 50 and 250
          ))
                        """
    ),
    (
        "query14_p1",
        """


        with cross_items as
                                                                         (select i_item_sk ss_item_sk
                                                                          from {ITEM_TABLE},
                                                                               (select iss.i_brand_id brand_id
                                                                                     ,iss.i_class_id class_id
                                                                                     ,iss.i_category_id category_id
                                                                                from {STORE_SALES_TABLE}
                                                                                   ,{ITEM_TABLE} iss
                                                                                   ,{DATE_DIM_TABLE} d1
                                                                                where ss_item_sk = iss.i_item_sk
                                                                                  and ss_sold_date_sk = d1.d_date_sk
                                                                                  and d1.d_year between 1998 AND 1998 + 2
                                                                                intersect
                                                                                select ics.i_brand_id
                                                                                     ,ics.i_class_id
                                                                                     ,ics.i_category_id
                                                                                from {CATALOG_SALES_TABLE}
                                                                                   ,{ITEM_TABLE} ics
                                                                                   ,{DATE_DIM_TABLE} d2
                                                                                where cs_item_sk = ics.i_item_sk
                                                                                  and cs_sold_date_sk = d2.d_date_sk
                                                                                  and d2.d_year between 1998 AND 1998 + 2
                                                                                intersect
                                                                                select iws.i_brand_id
                                                                                     ,iws.i_class_id
                                                                                     ,iws.i_category_id
                                                                                from {WEB_SALES_TABLE}
                                                                                   ,{ITEM_TABLE} iws
                                                                                   ,{DATE_DIM_TABLE} d3
                                                                                where ws_item_sk = iws.i_item_sk
                                                                                  and ws_sold_date_sk = d3.d_date_sk
                                                                                  and d3.d_year between 1998 AND 1998 + 2)
                                                                          where i_brand_id = brand_id
                                                                            and i_class_id = class_id
                                                                            and i_category_id = category_id
                                                                         ),
                                                                     avg_sales as
                                                                         (select avg(quantity*list_price) average_sales
                                                                          from (select ss_quantity quantity
                                                                                     ,ss_list_price list_price
                                                                                from {STORE_SALES_TABLE}
                                                                                   ,{DATE_DIM_TABLE}
                                                                                where ss_sold_date_sk = d_date_sk
                                                                                  and d_year between 1998 and 1998 + 2
                                                                                union all
                                                                                select cs_quantity quantity
                                                                                     ,cs_list_price list_price
                                                                                from {CATALOG_SALES_TABLE}
                                                                                   ,{DATE_DIM_TABLE}
                                                                                where cs_sold_date_sk = d_date_sk
                                                                                  and d_year between 1998 and 1998 + 2
                                                                                union all
                                                                                select ws_quantity quantity
                                                                                     ,ws_list_price list_price
                                                                                from {WEB_SALES_TABLE}
                                                                                   ,{DATE_DIM_TABLE}
                                                                                where ws_sold_date_sk = d_date_sk
                                                                                  and d_year between 1998 and 1998 + 2) x)
select  channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
from(
        select '{STORE_TABLE}' channel, i_brand_id,i_class_id
             ,i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
        from {STORE_SALES_TABLE}
           ,{ITEM_TABLE}
           ,{DATE_DIM_TABLE}
        where ss_item_sk in (select ss_item_sk from cross_items)
          and ss_item_sk = i_item_sk
          and ss_sold_date_sk = d_date_sk
          and d_year = 1998+2
          and d_moy = 11
        group by i_brand_id,i_class_id,i_category_id
        having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
        union all
        select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
        from {CATALOG_SALES_TABLE}
           ,{ITEM_TABLE}
           ,{DATE_DIM_TABLE}
        where cs_item_sk in (select ss_item_sk from cross_items)
          and cs_item_sk = i_item_sk
          and cs_sold_date_sk = d_date_sk
          and d_year = 1998+2
          and d_moy = 11
        group by i_brand_id,i_class_id,i_category_id
        having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
        union all
        select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
        from {WEB_SALES_TABLE}
           ,{ITEM_TABLE}
           ,{DATE_DIM_TABLE}
        where ws_item_sk in (select ss_item_sk from cross_items)
          and ws_item_sk = i_item_sk
          and ws_sold_date_sk = d_date_sk
          and d_year = 1998+2
          and d_moy = 11
        group by i_brand_id,i_class_id,i_category_id
        having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
    ) y
group by rollup (channel, i_brand_id,i_class_id,i_category_id)
order by channel,i_brand_id,i_class_id,i_category_id
    limit 100
                        """
    ),
    (
        "query14_p2",
        """


        with cross_items as
                                                                         (select i_item_sk ss_item_sk
                                                                          from {ITEM_TABLE},
                                                                               (select iss.i_brand_id brand_id
                                                                                     ,iss.i_class_id class_id
                                                                                     ,iss.i_category_id category_id
                                                                                from {STORE_SALES_TABLE}
                                                                                   ,{ITEM_TABLE} iss
                                                                                   ,{DATE_DIM_TABLE} d1
                                                                                where ss_item_sk = iss.i_item_sk
                                                                                  and ss_sold_date_sk = d1.d_date_sk
                                                                                  and d1.d_year between 1998 AND 1998 + 2
                                                                                intersect
                                                                                select ics.i_brand_id
                                                                                     ,ics.i_class_id
                                                                                     ,ics.i_category_id
                                                                                from {CATALOG_SALES_TABLE}
                                                                                   ,{ITEM_TABLE} ics
                                                                                   ,{DATE_DIM_TABLE} d2
                                                                                where cs_item_sk = ics.i_item_sk
                                                                                  and cs_sold_date_sk = d2.d_date_sk
                                                                                  and d2.d_year between 1998 AND 1998 + 2
                                                                                intersect
                                                                                select iws.i_brand_id
                                                                                     ,iws.i_class_id
                                                                                     ,iws.i_category_id
                                                                                from {WEB_SALES_TABLE}
                                                                                   ,{ITEM_TABLE} iws
                                                                                   ,{DATE_DIM_TABLE} d3
                                                                                where ws_item_sk = iws.i_item_sk
                                                                                  and ws_sold_date_sk = d3.d_date_sk
                                                                                  and d3.d_year between 1998 AND 1998 + 2) x
                                                                          where i_brand_id = brand_id
                                                                            and i_class_id = class_id
                                                                            and i_category_id = category_id
                                                                         ),
                                                                     avg_sales as
                                                                         (select avg(quantity*list_price) average_sales
                                                                          from (select ss_quantity quantity
                                                                                     ,ss_list_price list_price
                                                                                from {STORE_SALES_TABLE}
                                                                                   ,{DATE_DIM_TABLE}
                                                                                where ss_sold_date_sk = d_date_sk
                                                                                  and d_year between 1998 and 1998 + 2
                                                                                union all
                                                                                select cs_quantity quantity
                                                                                     ,cs_list_price list_price
                                                                                from {CATALOG_SALES_TABLE}
                                                                                   ,{DATE_DIM_TABLE}
                                                                                where cs_sold_date_sk = d_date_sk
                                                                                  and d_year between 1998 and 1998 + 2
                                                                                union all
                                                                                select ws_quantity quantity
                                                                                     ,ws_list_price list_price
                                                                                from {WEB_SALES_TABLE}
                                                                                   ,{DATE_DIM_TABLE}
                                                                                where ws_sold_date_sk = d_date_sk
                                                                                  and d_year between 1998 and 1998 + 2) x)
select  this_year.channel ty_channel
     ,this_year.i_brand_id ty_brand
     ,this_year.i_class_id ty_class
     ,this_year.i_category_id ty_category
     ,this_year.sales ty_sales
     ,this_year.number_sales ty_number_sales
     ,last_year.channel ly_channel
     ,last_year.i_brand_id ly_brand
     ,last_year.i_class_id ly_class
     ,last_year.i_category_id ly_category
     ,last_year.sales ly_sales
     ,last_year.number_sales ly_number_sales
from
    (select '{STORE_TABLE}' channel, i_brand_id,i_class_id,i_category_id
          ,sum(ss_quantity*ss_list_price) sales, count(*) number_sales
     from {STORE_SALES_TABLE}
        ,{ITEM_TABLE}
        ,{DATE_DIM_TABLE}
     where ss_item_sk in (select ss_item_sk from cross_items)
       and ss_item_sk = i_item_sk
       and ss_sold_date_sk = d_date_sk
       and d_week_seq = (select d_week_seq
                         from {DATE_DIM_TABLE}
                         where d_year = 1998 + 1
                           and d_moy = 12
                           and d_dom = 28)
     group by i_brand_id,i_class_id,i_category_id
     having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) this_year,
    (select '{STORE_TABLE}' channel, i_brand_id,i_class_id
          ,i_category_id, sum(ss_quantity*ss_list_price) sales, count(*) number_sales
     from {STORE_SALES_TABLE}
        ,{ITEM_TABLE}
        ,{DATE_DIM_TABLE}
     where ss_item_sk in (select ss_item_sk from cross_items)
       and ss_item_sk = i_item_sk
       and ss_sold_date_sk = d_date_sk
       and d_week_seq = (select d_week_seq
                         from {DATE_DIM_TABLE}
                         where d_year = 1998
                           and d_moy = 12
                           and d_dom = 28)
     group by i_brand_id,i_class_id,i_category_id
     having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) last_year
where this_year.i_brand_id= last_year.i_brand_id
  and this_year.i_class_id = last_year.i_class_id
  and this_year.i_category_id = last_year.i_category_id
order by this_year.channel, this_year.i_brand_id, this_year.i_class_id, this_year.i_category_id
    limit 100
                        """
    ),
    (
        "query15",
        """


        select ca_zip
     ,sum(cs_sales_price)
from {CATALOG_SALES_TABLE}
   ,{CUSTOMER_TABLE}
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{DATE_DIM_TABLE}
where cs_bill_customer_sk = c_customer_sk
  and c_current_addr_sk = ca_address_sk
  and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                               '85392', '85460', '80348', '81792')
    or ca_state in ('CA','WA','GA')
    or cs_sales_price > 500)
  and cs_sold_date_sk = d_date_sk
  and d_qoy = 2 and d_year = 2000
group by ca_zip
order by ca_zip
    limit 100
                        """
    ),
    (
        "query16",
        """


        select count(distinct cs_order_number) as "order count"
     ,sum(cs_ext_ship_cost) as "total shipping cost"
     ,sum(cs_net_profit) as "total net profit"
from
    {CATALOG_SALES_TABLE} cs1
   ,{DATE_DIM_TABLE}
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{CALL_CENTER_TABLE}
where
    d_date between '2000-3-01' and
        dateadd(day,60,to_date('2000-3-01'))
  and cs1.cs_ship_date_sk = d_date_sk
  and cs1.cs_ship_addr_sk = ca_address_sk
  and ca_state = 'AL'
  and cs1.cs_call_center_sk = cc_call_center_sk
  and cc_county in ('Luce County','Ziebach County','San Miguel County','Dauphin County',
                    'Marshall County'
    )
  and exists (select *
              from {CATALOG_SALES_TABLE} cs2
              where cs1.cs_order_number = cs2.cs_order_number
                and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
  and not exists(select *
                 from {CATALOG_RETURNS_TABLE} cr1
                 where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
    limit 100
                        """
    ),
    (
        "query17",
        """


        select i_item_id
     ,i_item_desc
     ,s_state
     ,count(ss_quantity) as store_sales_quantitycount
     ,avg(ss_quantity) as store_sales_quantityave
     ,stddev_samp(ss_quantity) as store_sales_quantitystdev
     ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
     ,count(sr_return_quantity) as store_returns_quantitycount
     ,avg(sr_return_quantity) as store_returns_quantityave
     ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
     ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
     ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
     ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
     ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
from {STORE_SALES_TABLE}
   ,{STORE_RETURNS_TABLE}
   ,{CATALOG_SALES_TABLE}
   ,{DATE_DIM_TABLE} d1
   ,{DATE_DIM_TABLE} d2
   ,{DATE_DIM_TABLE} d3
   ,{STORE_TABLE}
   ,{ITEM_TABLE}
where d1.d_quarter_name = '1999Q1'
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk
  and ss_customer_sk = sr_customer_sk
  and ss_item_sk = sr_item_sk
  and ss_ticket_number = sr_ticket_number
  and sr_returned_date_sk = d2.d_date_sk
  and d2.d_quarter_name in ('1999Q1','1999Q2','1999Q3')
  and sr_customer_sk = cs_bill_customer_sk
  and sr_item_sk = cs_item_sk
  and cs_sold_date_sk = d3.d_date_sk
  and d3.d_quarter_name in ('1999Q1','1999Q2','1999Q3')
group by i_item_id
       ,i_item_desc
       ,s_state
order by i_item_id
       ,i_item_desc
       ,s_state
    limit 100
                        """
    ),
    (
        "query18",
        """


        select i_item_id,
                                                                    ca_country,
                                                                    ca_state,
                                                                    ca_county,
                                                                    avg( cast(cs_quantity as decimal(12,2))) agg1,
                                                                    avg( cast(cs_list_price as decimal(12,2))) agg2,
                                                                    avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
                                                                    avg( cast(cs_sales_price as decimal(12,2))) agg4,
                                                                    avg( cast(cs_net_profit as decimal(12,2))) agg5,
                                                                    avg( cast(c_birth_year as decimal(12,2))) agg6,
                                                                    avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
from {CATALOG_SALES_TABLE}, {CUSTOMER_DEMOGRAPHICS_TABLE} cd1,
     {CUSTOMER_DEMOGRAPHICS_TABLE} cd2, {CUSTOMER_TABLE}, {CUSTOMER_ADDRESS_TABLE}, {DATE_DIM_TABLE}, {ITEM_TABLE}
where cs_sold_date_sk = d_date_sk and
        cs_item_sk = i_item_sk and
        cs_bill_cdemo_sk = cd1.cd_demo_sk and
        cs_bill_customer_sk = c_customer_sk and
        cd1.cd_gender = 'F' and
        cd1.cd_education_status = 'Secondary' and
        c_current_cdemo_sk = cd2.cd_demo_sk and
        c_current_addr_sk = ca_address_sk and
        c_birth_month in (1,9,11,4,12,6) and
        d_year = 1998 and
        ca_state in ('IL','WA','KY'
        ,'ND','VT','NC','NJ')
group by rollup (i_item_id, ca_country, ca_state, ca_county)
order by ca_country,
         ca_state,
         ca_county,
         i_item_id
    limit 100
                        """
    ),
    (
        "query19",
        """


        select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
                                                                   sum(ss_ext_sales_price) ext_price
from {DATE_DIM_TABLE}, {STORE_SALES_TABLE}, {ITEM_TABLE},{CUSTOMER_TABLE},{CUSTOMER_ADDRESS_TABLE},{STORE_TABLE}
where d_date_sk = ss_sold_date_sk
  and ss_item_sk = i_item_sk
  and i_manager_id=8
  and d_moy=11
  and d_year=1999
  and ss_customer_sk = c_customer_sk
  and c_current_addr_sk = ca_address_sk
  and substr(ca_zip,1,5) <> substr(s_zip,1,5)
  and ss_store_sk = s_store_sk
group by i_brand
       ,i_brand_id
       ,i_manufact_id
       ,i_manufact
order by ext_price desc
       ,i_brand
       ,i_brand_id
       ,i_manufact_id
       ,i_manufact
    limit 100
                        """
    ),
    (
        "query20",
        """


        select i_item_id
     ,i_item_desc
     ,i_category
     ,i_class
     ,i_current_price
     ,sum(cs_ext_sales_price) as itemrevenue
     ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
    (partition by i_class) as revenueratio
from {CATALOG_SALES_TABLE}
   ,{ITEM_TABLE}
   ,{DATE_DIM_TABLE}
where cs_item_sk = i_item_sk
  and i_category in ('Men', 'Electronics', 'Music')
  and cs_sold_date_sk = d_date_sk
  and d_date between cast('2001-03-09' as date)
    and dateadd(day,30,to_date('2001-03-09'))
group by i_item_id
       ,i_item_desc
       ,i_category
       ,i_class
       ,i_current_price
order by i_category
       ,i_class
       ,i_item_id
       ,i_item_desc
       ,revenueratio
    limit 100
                        """
    ),
    (
        "query21",
        """


        select *
from(select w_warehouse_name
          ,i_item_id
          ,sum(case when (cast(d_date as date) < cast ('2000-05-19' as date))
                        then inv_quantity_on_hand
                    else 0 end) as inv_before
          ,sum(case when (cast(d_date as date) >= cast ('2000-05-19' as date))
                        then inv_quantity_on_hand
                    else 0 end) as inv_after
     from {INVENTORY_TABLE}
        ,{WAREHOUSE_TABLE}
        ,{ITEM_TABLE}
        ,{DATE_DIM_TABLE}
     where i_current_price between 0.99 and 1.49
       and i_item_sk          = inv_item_sk
       and inv_warehouse_sk   = w_warehouse_sk
       and inv_date_sk    = d_date_sk
       and d_date between dateadd(day,-30,to_date('2000-05-19'))
         and dateadd(day,30,to_date('2000-05-19'))
     group by w_warehouse_name, i_item_id) x
where (case when inv_before > 0
                then inv_after / inv_before
            else null
    end) between 2.0/3.0 and 3.0/2.0
order by w_warehouse_name
       ,i_item_id
    limit 100
                        """
    ),
    (
        "query22",
        """


        select i_product_name
     ,i_brand
     ,i_class
     ,i_category
     ,avg(inv_quantity_on_hand) qoh
from {INVENTORY_TABLE}
   ,{DATE_DIM_TABLE}
   ,{ITEM_TABLE}
where inv_date_sk=d_date_sk
  and inv_item_sk=i_item_sk
  and d_month_seq between 1190 and 1190 + 11
group by rollup(i_product_name
       ,i_brand
       ,i_class
       ,i_category)
order by qoh, i_product_name, i_brand, i_class, i_category
    limit 100
                        """
    ),
    (
        "query23_p1",
        """


        with frequent_ss_items as
                                                                        (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
                                                                         from {STORE_SALES_TABLE}
                                                                            ,{DATE_DIM_TABLE}
                                                                            ,{ITEM_TABLE}
                                                                         where ss_sold_date_sk = d_date_sk
                                                                           and ss_item_sk = i_item_sk
                                                                           and d_year in (2000,2000+1,2000+2,2000+3)
                                                                         group by substr(i_item_desc,1,30),i_item_sk,d_date
                                                                         having count(*) >4),
                                                                    max_store_sales as
                                                                        (select max(csales) tpcds_cmax
                                                                         from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
                                                                               from {STORE_SALES_TABLE}
                                                                                  ,{CUSTOMER_TABLE}
                                                                                  ,{DATE_DIM_TABLE}
                                                                               where ss_customer_sk = c_customer_sk
                                                                                 and ss_sold_date_sk = d_date_sk
                                                                                 and d_year in (2000,2000+1,2000+2,2000+3)
                                                                               group by c_customer_sk)),
                                                                    best_ss_customer as
                                                                        (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
                                                                         from {STORE_SALES_TABLE}
                                                                            ,{CUSTOMER_TABLE}
                                                                         where ss_customer_sk = c_customer_sk
                                                                         group by c_customer_sk
                                                                         having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
                                                                                                                                    *
                                                                                                                                from
                                                                                                                                    max_store_sales))
select  sum(sales)
from (select cs_quantity*cs_list_price sales
      from {CATALOG_SALES_TABLE}
         ,{DATE_DIM_TABLE}
      where d_year = 2000
        and d_moy = 2
        and cs_sold_date_sk = d_date_sk
        and cs_item_sk in (select item_sk from frequent_ss_items)
        and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
      from {WEB_SALES_TABLE}
         ,{DATE_DIM_TABLE}
      where d_year = 2000
        and d_moy = 2
        and ws_sold_date_sk = d_date_sk
        and ws_item_sk in (select item_sk from frequent_ss_items)
        and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))
         limit 100
                        """
    ),
    (
        "query23_p2",
        """


        with frequent_ss_items as
                                                                        (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
                                                                         from {STORE_SALES_TABLE}
                                                                            ,{DATE_DIM_TABLE}
                                                                            ,{ITEM_TABLE}
                                                                         where ss_sold_date_sk = d_date_sk
                                                                           and ss_item_sk = i_item_sk
                                                                           and d_year in (2000,2000 + 1,2000 + 2,2000 + 3)
                                                                         group by substr(i_item_desc,1,30),i_item_sk,d_date
                                                                         having count(*) >4),
                                                                    max_store_sales as
                                                                        (select max(csales) tpcds_cmax
                                                                         from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
                                                                               from {STORE_SALES_TABLE}
                                                                                  ,{CUSTOMER_TABLE}
                                                                                  ,{DATE_DIM_TABLE}
                                                                               where ss_customer_sk = c_customer_sk
                                                                                 and ss_sold_date_sk = d_date_sk
                                                                                 and d_year in (2000,2000+1,2000+2,2000+3)
                                                                               group by c_customer_sk)),
                                                                    best_ss_customer as
                                                                        (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
                                                                         from {STORE_SALES_TABLE}
                                                                            ,{CUSTOMER_TABLE}
                                                                         where ss_customer_sk = c_customer_sk
                                                                         group by c_customer_sk
                                                                         having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
                                                                                                                                    *
                                                                                                                                from max_store_sales))
select  c_last_name,c_first_name,sales
from (select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
      from {CATALOG_SALES_TABLE}
         ,{CUSTOMER_TABLE}
         ,{DATE_DIM_TABLE}
      where d_year = 2000
        and d_moy = 2
        and cs_sold_date_sk = d_date_sk
        and cs_item_sk in (select item_sk from frequent_ss_items)
        and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
        and cs_bill_customer_sk = c_customer_sk
      group by c_last_name,c_first_name
      union all
      select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
      from {WEB_SALES_TABLE}
         ,{CUSTOMER_TABLE}
         ,{DATE_DIM_TABLE}
      where d_year = 2000
        and d_moy = 2
        and ws_sold_date_sk = d_date_sk
        and ws_item_sk in (select item_sk from frequent_ss_items)
        and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
        and ws_bill_customer_sk = c_customer_sk
      group by c_last_name,c_first_name)
order by c_last_name,c_first_name,sales
    limit 100
                        """
    ),
    (
        "query24_p1",
        """


        with ssales as
                                                                        (select c_last_name
                                                                              ,c_first_name
                                                                              ,s_store_name
                                                                              ,ca_state
                                                                              ,s_state
                                                                              ,i_color
                                                                              ,i_current_price
                                                                              ,i_manager_id
                                                                              ,i_units
                                                                              ,i_size
                                                                              ,sum(ss_net_paid_inc_tax) netpaid
                                                                         from {STORE_SALES_TABLE}
                                                                            ,{STORE_RETURNS_TABLE}
                                                                            ,{STORE_TABLE}
                                                                            ,{ITEM_TABLE}
                                                                            ,{CUSTOMER_TABLE}
                                                                            ,{CUSTOMER_ADDRESS_TABLE}
                                                                         where ss_ticket_number = sr_ticket_number
                                                                           and ss_item_sk = sr_item_sk
                                                                           and ss_customer_sk = c_customer_sk
                                                                           and ss_item_sk = i_item_sk
                                                                           and ss_store_sk = s_store_sk
                                                                           and c_current_addr_sk = ca_address_sk
                                                                           and c_birth_country <> upper(ca_country)
                                                                           and s_zip = ca_zip
                                                                           and s_market_id=8
                                                                         group by c_last_name
                                                                                ,c_first_name
                                                                                ,s_store_name
                                                                                ,ca_state
                                                                                ,s_state
                                                                                ,i_color
                                                                                ,i_current_price
                                                                                ,i_manager_id
                                                                                ,i_units
                                                                                ,i_size)
select c_last_name
     ,c_first_name
     ,s_store_name
     ,sum(netpaid) paid
from ssales
where i_color = 'cornsilk'
group by c_last_name
       ,c_first_name
       ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid)
                       from ssales)
order by c_last_name
       ,c_first_name
       ,s_store_name
                        """
    ),
    (
        "query24_p2",
        """


        with ssales as
                                                                        (select c_last_name
                                                                              ,c_first_name
                                                                              ,s_store_name
                                                                              ,ca_state
                                                                              ,s_state
                                                                              ,i_color
                                                                              ,i_current_price
                                                                              ,i_manager_id
                                                                              ,i_units
                                                                              ,i_size
                                                                              ,sum(ss_net_paid_inc_tax) netpaid
                                                                         from {STORE_SALES_TABLE}
                                                                            ,{STORE_RETURNS_TABLE}
                                                                            ,{STORE_TABLE}
                                                                            ,{ITEM_TABLE}
                                                                            ,{CUSTOMER_TABLE}
                                                                            ,{CUSTOMER_ADDRESS_TABLE}
                                                                         where ss_ticket_number = sr_ticket_number
                                                                           and ss_item_sk = sr_item_sk
                                                                           and ss_customer_sk = c_customer_sk
                                                                           and ss_item_sk = i_item_sk
                                                                           and ss_store_sk = s_store_sk
                                                                           and c_current_addr_sk = ca_address_sk
                                                                           and c_birth_country <> upper(ca_country)
                                                                           and s_zip = ca_zip
                                                                           and s_market_id = 8
                                                                         group by c_last_name
                                                                                ,c_first_name
                                                                                ,s_store_name
                                                                                ,ca_state
                                                                                ,s_state
                                                                                ,i_color
                                                                                ,i_current_price
                                                                                ,i_manager_id
                                                                                ,i_units
                                                                                ,i_size)
select c_last_name
     ,c_first_name
     ,s_store_name
     ,sum(netpaid) paid
from ssales
where i_color = 'lime'
group by c_last_name
       ,c_first_name
       ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid)
                       from ssales)
order by c_last_name
       ,c_first_name
       ,s_store_name
                        """
    ),
    (
        "query25",
        """


        select i_item_id
     ,i_item_desc
     ,s_store_id
     ,s_store_name
     ,min(ss_net_profit) as store_sales_profit
     ,min(sr_net_loss) as store_returns_loss
     ,min(cs_net_profit) as catalog_sales_profit
from
    {STORE_SALES_TABLE}
   ,{STORE_RETURNS_TABLE}
   ,{CATALOG_SALES_TABLE}
   ,{DATE_DIM_TABLE} d1
   ,{DATE_DIM_TABLE} d2
   ,{DATE_DIM_TABLE} d3
   ,{STORE_TABLE}
   ,{ITEM_TABLE}
where
        d1.d_moy = 4
  and d1.d_year = 2002
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk
  and ss_customer_sk = sr_customer_sk
  and ss_item_sk = sr_item_sk
  and ss_ticket_number = sr_ticket_number
  and sr_returned_date_sk = d2.d_date_sk
  and d2.d_moy               between 4 and  10
  and d2.d_year              = 2002
  and sr_customer_sk = cs_bill_customer_sk
  and sr_item_sk = cs_item_sk
  and cs_sold_date_sk = d3.d_date_sk
  and d3.d_moy               between 4 and  10
  and d3.d_year              = 2002
group by
    i_item_id
       ,i_item_desc
       ,s_store_id
       ,s_store_name
order by
    i_item_id
       ,i_item_desc
       ,s_store_id
       ,s_store_name
    limit 100
                        """
    ),
    (
        "query26",
        """


        select i_item_id,
                                                                    avg(cs_quantity) agg1,
                                                                    avg(cs_list_price) agg2,
                                                                    avg(cs_coupon_amt) agg3,
                                                                    avg(cs_sales_price) agg4
from {CATALOG_SALES_TABLE}, {CUSTOMER_DEMOGRAPHICS_TABLE}, {DATE_DIM_TABLE}, {ITEM_TABLE}, {PROMOTION_TABLE}
where cs_sold_date_sk = d_date_sk and
        cs_item_sk = i_item_sk and
        cs_bill_cdemo_sk = cd_demo_sk and
        cs_promo_sk = p_promo_sk and
        cd_gender = 'M' and
        cd_marital_status = 'S' and
        cd_education_status = '4 yr Degree' and
    (p_channel_email = 'N' or p_channel_event = 'N') and
        d_year = 1998
group by i_item_id
order by i_item_id
    limit 100
                        """
    ),
    (
        "query27",
        """


        select i_item_id,
                                                                    s_state, grouping(s_state) g_state,
                                                                    avg(ss_quantity) agg1,
                                                                    avg(ss_list_price) agg2,
                                                                    avg(ss_coupon_amt) agg3,
                                                                    avg(ss_sales_price) agg4
from {STORE_SALES_TABLE}, {CUSTOMER_DEMOGRAPHICS_TABLE}, {DATE_DIM_TABLE}, {STORE_TABLE}, {ITEM_TABLE}
where ss_sold_date_sk = d_date_sk and
        ss_item_sk = i_item_sk and
        ss_store_sk = s_store_sk and
        ss_cdemo_sk = cd_demo_sk and
        cd_gender = 'F' and
        cd_marital_status = 'U' and
        cd_education_status = '2 yr Degree' and
        d_year = 2000 and
        s_state in ('SD','OH', 'NM', 'MI', 'TX', 'NM')
group by rollup (i_item_id, s_state)
order by i_item_id
       ,s_state
    limit 100
                        """
    ),
    (
        "query28",
        """


        select *
from (select avg(ss_list_price) B1_LP
           ,count(ss_list_price) B1_CNT
           ,count(distinct ss_list_price) B1_CNTD
      from {STORE_SALES_TABLE}
      where ss_quantity between 0 and 5
        and (ss_list_price between 173 and 173+10
          or ss_coupon_amt between 12318 and 12318+1000
          or ss_wholesale_cost between 4 and 4+20)) B1,
     (select avg(ss_list_price) B2_LP
           ,count(ss_list_price) B2_CNT
           ,count(distinct ss_list_price) B2_CNTD
      from {STORE_SALES_TABLE}
      where ss_quantity between 6 and 10
        and (ss_list_price between 148 and 148+10
          or ss_coupon_amt between 15913 and 15913+1000
          or ss_wholesale_cost between 54 and 54+20)) B2,
     (select avg(ss_list_price) B3_LP
           ,count(ss_list_price) B3_CNT
           ,count(distinct ss_list_price) B3_CNTD
      from {STORE_SALES_TABLE}
      where ss_quantity between 11 and 15
        and (ss_list_price between 114 and 114+10
          or ss_coupon_amt between 10419 and 10419+1000
          or ss_wholesale_cost between 20 and 20+20)) B3,
     (select avg(ss_list_price) B4_LP
           ,count(ss_list_price) B4_CNT
           ,count(distinct ss_list_price) B4_CNTD
      from {STORE_SALES_TABLE}
      where ss_quantity between 16 and 20
        and (ss_list_price between 120 and 120+10
          or ss_coupon_amt between 13066 and 13066+1000
          or ss_wholesale_cost between 41 and 41+20)) B4,
     (select avg(ss_list_price) B5_LP
           ,count(ss_list_price) B5_CNT
           ,count(distinct ss_list_price) B5_CNTD
      from {STORE_SALES_TABLE}
      where ss_quantity between 21 and 25
        and (ss_list_price between 42 and 42+10
          or ss_coupon_amt between 5802 and 5802+1000
          or ss_wholesale_cost between 8 and 8+20)) B5,
     (select avg(ss_list_price) B6_LP
           ,count(ss_list_price) B6_CNT
           ,count(distinct ss_list_price) B6_CNTD
      from {STORE_SALES_TABLE}
      where ss_quantity between 26 and 30
        and (ss_list_price between 22 and 22+10
          or ss_coupon_amt between 281 and 281+1000
          or ss_wholesale_cost between 48 and 48+20)) B6
    limit 100
                        """
    ),
    (
        "query29",
        """


        select i_item_id
     ,i_item_desc
     ,s_store_id
     ,s_store_name
     ,sum(ss_quantity)        as store_sales_quantity
     ,sum(sr_return_quantity) as store_returns_quantity
     ,sum(cs_quantity)        as catalog_sales_quantity
from
    {STORE_SALES_TABLE}
   ,{STORE_RETURNS_TABLE}
   ,{CATALOG_SALES_TABLE}
   ,{DATE_DIM_TABLE}             d1
   ,{DATE_DIM_TABLE}             d2
   ,{DATE_DIM_TABLE}             d3
   ,{STORE_TABLE}
   ,{ITEM_TABLE}
where
        d1.d_moy               = 4
  and d1.d_year              = 2000
  and d1.d_date_sk           = ss_sold_date_sk
  and i_item_sk              = ss_item_sk
  and s_store_sk             = ss_store_sk
  and ss_customer_sk         = sr_customer_sk
  and ss_item_sk             = sr_item_sk
  and ss_ticket_number       = sr_ticket_number
  and sr_returned_date_sk    = d2.d_date_sk
  and d2.d_moy               between 4 and  4 + 3
  and d2.d_year              = 2000
  and sr_customer_sk         = cs_bill_customer_sk
  and sr_item_sk             = cs_item_sk
  and cs_sold_date_sk        = d3.d_date_sk
  and d3.d_year              in (2000,2000+1,2000+2)
group by
    i_item_id
       ,i_item_desc
       ,s_store_id
       ,s_store_name
order by
    i_item_id
       ,i_item_desc
       ,s_store_id
       ,s_store_name
    limit 100
                        """
    ),
    (
        "query30",
        """


        with customer_total_return as
                                                                     (select wr_returning_customer_sk as ctr_customer_sk
                                                                              ,ca_state as ctr_state,
                                                                             sum(wr_return_amt) as ctr_total_return
                                                                      from {WEB_RETURNS_TABLE}
                                                                         ,{DATE_DIM_TABLE}
                                                                         ,{CUSTOMER_ADDRESS_TABLE}
                                                                      where wr_returned_date_sk = d_date_sk
                                                                        and d_year =2001
                                                                        and wr_returning_addr_sk = ca_address_sk
                                                                      group by wr_returning_customer_sk
                                                                             ,ca_state)
select  c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
     ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
     ,c_last_review_date,ctr_total_return
from customer_total_return ctr1
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{CUSTOMER_TABLE}
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
                               from customer_total_return ctr2
                               where ctr1.ctr_state = ctr2.ctr_state)
  and ca_address_sk = c_current_addr_sk
  and ca_state = 'MI'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       ,c_last_review_date,ctr_total_return
    limit 100
                        """
    ),
    (
        "query31",
        """


        with ss as
                                                                     (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as {STORE_SALES_TABLE}
                                                                      from {STORE_SALES_TABLE},{DATE_DIM_TABLE},{CUSTOMER_ADDRESS_TABLE}
                                                                      where ss_sold_date_sk = d_date_sk
                                                                        and ss_addr_sk=ca_address_sk
                                                                      group by ca_county,d_qoy, d_year),
                                                                 ws as
                                                                     (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as {WEB_SALES_TABLE}
                                                                      from {WEB_SALES_TABLE},{DATE_DIM_TABLE},{CUSTOMER_ADDRESS_TABLE}
                                                                      where ws_sold_date_sk = d_date_sk
                                                                        and ws_bill_addr_sk=ca_address_sk
                                                                      group by ca_county,d_qoy, d_year)
select
    ss1.ca_county
     ,ss1.d_year
     ,ws2.{WEB_SALES_TABLE}/ws1.{WEB_SALES_TABLE} web_q1_q2_increase
     ,ss2.{STORE_SALES_TABLE}/ss1.{STORE_SALES_TABLE} store_q1_q2_increase
     ,ws3.{WEB_SALES_TABLE}/ws2.{WEB_SALES_TABLE} web_q2_q3_increase
     ,ss3.{STORE_SALES_TABLE}/ss2.{STORE_SALES_TABLE} store_q2_q3_increase
from
    ss ss1
   ,ss ss2
   ,ss ss3
   ,ws ws1
   ,ws ws2
   ,ws ws3
where
        ss1.d_qoy = 1
  and ss1.d_year = 1999
  and ss1.ca_county = ss2.ca_county
  and ss2.d_qoy = 2
  and ss2.d_year = 1999
  and ss2.ca_county = ss3.ca_county
  and ss3.d_qoy = 3
  and ss3.d_year = 1999
  and ss1.ca_county = ws1.ca_county
  and ws1.d_qoy = 1
  and ws1.d_year = 1999
  and ws1.ca_county = ws2.ca_county
  and ws2.d_qoy = 2
  and ws2.d_year = 1999
  and ws1.ca_county = ws3.ca_county
  and ws3.d_qoy = 3
  and ws3.d_year =1999
  and case when ws1.{WEB_SALES_TABLE} > 0 then ws2.{WEB_SALES_TABLE}/ws1.{WEB_SALES_TABLE} else null end
    > case when ss1.{STORE_SALES_TABLE} > 0 then ss2.{STORE_SALES_TABLE}/ss1.{STORE_SALES_TABLE} else null end
  and case when ws2.{WEB_SALES_TABLE} > 0 then ws3.{WEB_SALES_TABLE}/ws2.{WEB_SALES_TABLE} else null end
    > case when ss2.{STORE_SALES_TABLE} > 0 then ss3.{STORE_SALES_TABLE}/ss2.{STORE_SALES_TABLE} else null end
order by web_q1_q2_increase
                        """
    ),
    (
        "query32",
        """


        select sum(cs_ext_discount_amt)  as "excess discount amount"
from
    {CATALOG_SALES_TABLE}
   ,{ITEM_TABLE}
   ,{DATE_DIM_TABLE}
where
        i_manufact_id = 283
  and i_item_sk = cs_item_sk
  and d_date between '1999-02-22' and
    dateadd(day,90,to_date('1999-02-22'))
  and d_date_sk = cs_sold_date_sk
  and cs_ext_discount_amt
    > (
          select
                  1.3 * avg(cs_ext_discount_amt)
          from
              {CATALOG_SALES_TABLE}
             ,{DATE_DIM_TABLE}
          where
                  cs_item_sk = i_item_sk
            and d_date between '1999-02-22' and
              dateadd(day,90,to_date('1999-02-22'))
            and d_date_sk = cs_sold_date_sk
      )
    limit 100
                        """
    ),
    (
        "query33",
        """


        with ss as (
    select
        i_manufact_id,sum(ss_ext_sales_price) total_sales
    from
        {STORE_SALES_TABLE},
        {DATE_DIM_TABLE},
        {CUSTOMER_ADDRESS_TABLE},
        {ITEM_TABLE}
    where
            i_manufact_id in (select
                                  i_manufact_id
                              from
                                  {ITEM_TABLE}
                              where i_category in ('Electronics'))
      and     ss_item_sk              = i_item_sk
      and     ss_sold_date_sk         = d_date_sk
      and     d_year                  = 1999
      and     d_moy                   = 7
      and     ss_addr_sk              = ca_address_sk
      and     ca_gmt_offset           = -5
    group by i_manufact_id),
                                                                 cs as (
                                                                     select
                                                                         i_manufact_id,sum(cs_ext_sales_price) total_sales
                                                                     from
                                                                         {CATALOG_SALES_TABLE},
                                                                         {DATE_DIM_TABLE},
                                                                         {CUSTOMER_ADDRESS_TABLE},
                                                                         {ITEM_TABLE}
                                                                     where
                                                                             i_manufact_id               in (select
                                                                                                                 i_manufact_id
                                                                                                             from
                                                                                                                 {ITEM_TABLE}
                                                                                                             where i_category in ('Electronics'))
                                                                       and     cs_item_sk              = i_item_sk
                                                                       and     cs_sold_date_sk         = d_date_sk
                                                                       and     d_year                  = 1999
                                                                       and     d_moy                   = 7
                                                                       and     cs_bill_addr_sk         = ca_address_sk
                                                                       and     ca_gmt_offset           = -5
                                                                     group by i_manufact_id),
                                                                 ws as (
                                                                     select
                                                                         i_manufact_id,sum(ws_ext_sales_price) total_sales
                                                                     from
                                                                         {WEB_SALES_TABLE},
                                                                         {DATE_DIM_TABLE},
                                                                         {CUSTOMER_ADDRESS_TABLE},
                                                                         {ITEM_TABLE}
                                                                     where
                                                                             i_manufact_id               in (select
                                                                                                                 i_manufact_id
                                                                                                             from
                                                                                                                 {ITEM_TABLE}
                                                                                                             where i_category in ('Electronics'))
                                                                       and     ws_item_sk              = i_item_sk
                                                                       and     ws_sold_date_sk         = d_date_sk
                                                                       and     d_year                  = 1999
                                                                       and     d_moy                   = 7
                                                                       and     ws_bill_addr_sk         = ca_address_sk
                                                                       and     ca_gmt_offset           = -5
                                                                     group by i_manufact_id)
select  i_manufact_id ,sum(total_sales) total_sales
from  (select * from ss
       union all
       select * from cs
       union all
       select * from ws) tmp1
group by i_manufact_id
order by total_sales
    limit 100
                        """
    ),
    (
        "query34",
        """


        select c_last_name
     ,c_first_name
     ,c_salutation
     ,c_preferred_cust_flag
     ,ss_ticket_number
     ,cnt from
              (select ss_ticket_number
                    ,ss_customer_sk
                    ,count(*) cnt
               from {STORE_SALES_TABLE},{DATE_DIM_TABLE},{STORE_TABLE},{HOUSEHOLD_DEMOGRAPHICS_TABLE}
               where {STORE_SALES_TABLE}.ss_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
                 and {STORE_SALES_TABLE}.ss_store_sk = {STORE_TABLE}.s_store_sk
                 and {STORE_SALES_TABLE}.ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
                 and ({DATE_DIM_TABLE}.d_dom between 1 and 3 or {DATE_DIM_TABLE}.d_dom between 25 and 28)
                 and ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_buy_potential = '1001-5000' or
                      {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_buy_potential = '0-500')
                 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count > 0
                 and (case when {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count > 0
                               then {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count/ {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count
                           else null
                   end)  > 1.2
                 and {DATE_DIM_TABLE}.d_year in (1998,1998+1,1998+2)
                 and {STORE_TABLE}.s_county in ('Quay County','Sumner County','Harper County','Pipestone County',
                                        'Kittitas County','Surry County','Wadena County','Richland County')
               group by ss_ticket_number,ss_customer_sk) dn,{CUSTOMER_TABLE}
where ss_customer_sk = c_customer_sk
  and cnt between 15 and 20
order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number
                        """
    ),
    (
        "query35",
        """


        select ca_state,
    cd_gender,
    cd_marital_status,
    cd_dep_count,
    count(*) cnt1,
    stddev_samp(cd_dep_count),
    max(cd_dep_count),
    sum(cd_dep_count),
    cd_dep_employed_count,
    count(*) cnt2,
    stddev_samp(cd_dep_employed_count),
    max(cd_dep_employed_count),
    sum(cd_dep_employed_count),
    cd_dep_college_count,
    count(*) cnt3,
    stddev_samp(cd_dep_college_count),
    max(cd_dep_college_count),
    sum(cd_dep_college_count)
from
    {CUSTOMER_TABLE} c,{CUSTOMER_ADDRESS_TABLE} ca,{CUSTOMER_DEMOGRAPHICS_TABLE}
where
        c.c_current_addr_sk = ca.ca_address_sk and
        cd_demo_sk = c.c_current_cdemo_sk and
    exists (select *
            from {STORE_SALES_TABLE},{DATE_DIM_TABLE}
            where c.c_customer_sk = ss_customer_sk and
                    ss_sold_date_sk = d_date_sk and
                    d_year = 2000 and
                    d_qoy < 4) and
    (exists (select *
             from {WEB_SALES_TABLE},{DATE_DIM_TABLE}
             where c.c_customer_sk = ws_bill_customer_sk and
                     ws_sold_date_sk = d_date_sk and
                     d_year = 2000 and
                     d_qoy < 4) or
     exists (select *
             from {CATALOG_SALES_TABLE},{DATE_DIM_TABLE}
             where c.c_customer_sk = cs_ship_customer_sk and
                     cs_sold_date_sk = d_date_sk and
                     d_year = 2000 and
                     d_qoy < 4))
group by ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
order by ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
    limit 100
                        """
    ),
    (
        "query36",
        """


        select sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
     ,i_category
     ,i_class
     ,grouping(i_category)+grouping(i_class) as lochierarchy
     ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
from
    {STORE_SALES_TABLE}
   ,{DATE_DIM_TABLE}       d1
   ,{ITEM_TABLE}
   ,{STORE_TABLE}
where
        d1.d_year = 1998
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk  = ss_item_sk
  and s_store_sk  = ss_store_sk
  and s_state in ('GA','AL','LA','NM',
                  'MN','NM','CO','MI')
group by rollup(i_category,i_class)
order by
    lochierarchy desc
       ,case when lochierarchy = 0 then i_category end
       ,rank_within_parent
    limit 100
                        """
    ),
    (
        "query37",
        """


        select i_item_id
     ,i_item_desc
     ,i_current_price
from {ITEM_TABLE}, {INVENTORY_TABLE}, {DATE_DIM_TABLE}, {CATALOG_SALES_TABLE}
where i_current_price between 58 and 58 + 30
  and inv_item_sk = i_item_sk
  and d_date_sk=inv_date_sk
  and d_date between cast('2001-02-11' as date) and dateadd(day,60,to_date('2001-02-11'))
  and i_manufact_id in (910,893,984,865)
  and inv_quantity_on_hand between 100 and 500
  and cs_item_sk = i_item_sk
group by i_item_id,i_item_desc,i_current_price
order by i_item_id
    limit 100
                        """
    ),
    (
        "query38",
        """


        select count(*) from (
                                                                                      select distinct c_last_name, c_first_name, d_date
                                                                                      from {STORE_SALES_TABLE}, {DATE_DIM_TABLE}, {CUSTOMER_TABLE}
                                                                                      where {STORE_SALES_TABLE}.ss_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
                                                                                        and {STORE_SALES_TABLE}.ss_customer_sk = {CUSTOMER_TABLE}.c_customer_sk
                                                                                        and d_month_seq between 1176 and 1176 + 11
                                                                                      intersect
                                                                                      select distinct c_last_name, c_first_name, d_date
                                                                                      from {CATALOG_SALES_TABLE}, {DATE_DIM_TABLE}, {CUSTOMER_TABLE}
                                                                                      where {CATALOG_SALES_TABLE}.cs_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
                                                                                        and {CATALOG_SALES_TABLE}.cs_bill_customer_sk = {CUSTOMER_TABLE}.c_customer_sk
                                                                                        and d_month_seq between 1176 and 1176 + 11
                                                                                      intersect
                                                                                      select distinct c_last_name, c_first_name, d_date
                                                                                      from {WEB_SALES_TABLE}, {DATE_DIM_TABLE}, {CUSTOMER_TABLE}
                                                                                      where {WEB_SALES_TABLE}.ws_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
                                                                                        and {WEB_SALES_TABLE}.ws_bill_customer_sk = {CUSTOMER_TABLE}.c_customer_sk
                                                                                        and d_month_seq between 1176 and 1176 + 11
                                                                                  ) hot_cust
    limit 100
                        """
    ),
    (
        "query39_p1",
        """


        with inv as
                                                                       (select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
                                                                             ,stdev,mean, case mean when 0 then null else stdev/mean end cov
                                                                        from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
                                                                                  ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
                                                                             from {INVENTORY_TABLE}
                                                                                ,{ITEM_TABLE}
                                                                                ,{WAREHOUSE_TABLE}
                                                                                ,{DATE_DIM_TABLE}
                                                                             where inv_item_sk = i_item_sk
                                                                               and inv_warehouse_sk = w_warehouse_sk
                                                                               and inv_date_sk = d_date_sk
                                                                               and d_year =2001
                                                                             group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
                                                                        where case mean when 0 then 0 else stdev/mean end > 1)
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
     ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv inv1,inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=1
  and inv2.d_moy=1+1
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
       ,inv2.d_moy,inv2.mean, inv2.cov
                        """
    ),
    (
        "query39_p2",
        """


        with inv as
                                                                       (select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
                                                                             ,stdev,mean, case mean when 0 then null else stdev/mean end cov
                                                                        from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
                                                                                  ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
                                                                             from {INVENTORY_TABLE}
                                                                                ,{ITEM_TABLE}
                                                                                ,{WAREHOUSE_TABLE}
                                                                                ,{DATE_DIM_TABLE}
                                                                             where inv_item_sk = i_item_sk
                                                                               and inv_warehouse_sk = w_warehouse_sk
                                                                               and inv_date_sk = d_date_sk
                                                                               and d_year =2001
                                                                             group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
                                                                        where case mean when 0 then 0 else stdev/mean end > 1)
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
     ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv inv1,inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=1
  and inv2.d_moy=1+1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
       ,inv2.d_moy,inv2.mean, inv2.cov
                        """
    ),
    (
        "query40",
        """


        select w_state
     ,i_item_id
     ,sum(case when (cast(d_date as date) < cast ('2002-05-08' as date))
                   then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
     ,sum(case when (cast(d_date as date) >= cast ('2002-05-08' as date))
                   then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
from
    {CATALOG_SALES_TABLE} left outer join {CATALOG_RETURNS_TABLE} on
        (cs_order_number = cr_order_number
            and cs_item_sk = cr_item_sk)
   ,{WAREHOUSE_TABLE}
   ,{ITEM_TABLE}
   ,{DATE_DIM_TABLE}
where
    i_current_price between 0.99 and 1.49
  and i_item_sk          = cs_item_sk
  and cs_warehouse_sk    = w_warehouse_sk
  and cs_sold_date_sk    = d_date_sk
  and d_date between dateadd(day,-30,to_date('2002-05-08')) and
    dateadd(day,30,to_date('2002-05-08'))
group by
    w_state,i_item_id
order by w_state,i_item_id
    limit 100
                        """
    ),
    (
        "query41",
        """


        select distinct(i_product_name)
from {ITEM_TABLE} i1
where i_manufact_id between 869 and 869+40
  and (select count(*) as item_cnt
       from {ITEM_TABLE}
       where (i_manufact = i1.i_manufact and
              ((i_category = 'Women' and
                (i_color = 'seashell' or i_color = 'indian') and
                (i_units = 'Carton' or i_units = 'Dozen') and
                (i_size = 'small' or i_size = 'extra large')
                   ) or
               (i_category = 'Women' and
                (i_color = 'thistle' or i_color = 'green') and
                (i_units = 'Box' or i_units = 'Tsp') and
                (i_size = 'N/A' or i_size = 'large')
                   ) or
               (i_category = 'Men' and
                (i_color = 'steel' or i_color = 'papaya') and
                (i_units = 'Dram' or i_units = 'Bundle') and
                (i_size = 'petite' or i_size = 'economy')
                   ) or
               (i_category = 'Men' and
                (i_color = 'pink' or i_color = 'black') and
                (i_units = 'Gross' or i_units = 'Oz') and
                (i_size = 'small' or i_size = 'extra large')
                   ))) or
           (i_manufact = i1.i_manufact and
            ((i_category = 'Women' and
              (i_color = 'lemon' or i_color = 'chocolate') and
              (i_units = 'Each' or i_units = 'N/A') and
              (i_size = 'small' or i_size = 'extra large')
                 ) or
             (i_category = 'Women' and
              (i_color = 'purple' or i_color = 'peru') and
              (i_units = 'Ounce' or i_units = 'Unknown') and
              (i_size = 'N/A' or i_size = 'large')
                 ) or
             (i_category = 'Men' and
              (i_color = 'rosy' or i_color = 'floral') and
              (i_units = 'Pound' or i_units = 'Ton') and
              (i_size = 'petite' or i_size = 'economy')
                 ) or
             (i_category = 'Men' and
              (i_color = 'plum' or i_color = 'firebrick') and
              (i_units = 'Bunch' or i_units = 'Gram') and
              (i_size = 'small' or i_size = 'extra large')
                 )))) > 0
order by i_product_name
    limit 100
                        """
    ),
    (
        "query42",
        """


        select dt.d_year
     ,{ITEM_TABLE}.i_category_id
     ,{ITEM_TABLE}.i_category
     ,sum(ss_ext_sales_price)
from 	{DATE_DIM_TABLE} dt
   ,{STORE_SALES_TABLE}
   ,{ITEM_TABLE}
where dt.d_date_sk = {STORE_SALES_TABLE}.ss_sold_date_sk
  and {STORE_SALES_TABLE}.ss_item_sk = {ITEM_TABLE}.i_item_sk
  and {ITEM_TABLE}.i_manager_id = 1
  and dt.d_moy=12
  and dt.d_year=2001
group by 	dt.d_year
       ,{ITEM_TABLE}.i_category_id
       ,{ITEM_TABLE}.i_category
order by       sum(ss_ext_sales_price) desc,dt.d_year
       ,{ITEM_TABLE}.i_category_id
       ,{ITEM_TABLE}.i_category
    limit 100
                        """
    ),
    (
        "query43",
        """


        select s_store_name, s_store_id,
                                                                    sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
                                                                    sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
                                                                    sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
                                                                    sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
                                                                    sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
                                                                    sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
                                                                    sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
from {DATE_DIM_TABLE}, {STORE_SALES_TABLE}, {STORE_TABLE}
where d_date_sk = ss_sold_date_sk and
        s_store_sk = ss_store_sk and
        s_gmt_offset = -6 and
        d_year = 2000
group by s_store_name, s_store_id
order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
    limit 100
                        """
    ),
    (
        "query44",
        """


        select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from {STORE_SALES_TABLE} ss1
                 where ss_store_sk = 114
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from {STORE_SALES_TABLE}
                                                  where ss_store_sk = 114
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from {STORE_SALES_TABLE} ss1
                 where ss_store_sk = 114
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from {STORE_SALES_TABLE}
                                                  where ss_store_sk = 114
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) descending,
    {ITEM_TABLE} i1,
    {ITEM_TABLE} i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
order by asceding.rnk
    limit 100
                        """
    ),
    (
        "query45",
        """


        select ca_zip, ca_city, sum(ws_sales_price)
from {WEB_SALES_TABLE}, {CUSTOMER_TABLE}, {CUSTOMER_ADDRESS_TABLE}, {DATE_DIM_TABLE}, {ITEM_TABLE}
where ws_bill_customer_sk = c_customer_sk
  and c_current_addr_sk = ca_address_sk
  and ws_item_sk = i_item_sk
  and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
    or
        i_item_id in (select i_item_id
                      from {ITEM_TABLE}
                      where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
        )
    )
  and ws_sold_date_sk = d_date_sk
  and d_qoy = 1 and d_year = 1998
group by ca_zip, ca_city
order by ca_zip, ca_city
    limit 100
                        """
    ),
    (
        "query46",
        """


        select c_last_name
     ,c_first_name
     ,ca_city
     ,bought_city
     ,ss_ticket_number
     ,amt,profit
from
    (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
     from {STORE_SALES_TABLE},{DATE_DIM_TABLE},{STORE_TABLE},{HOUSEHOLD_DEMOGRAPHICS_TABLE},{CUSTOMER_ADDRESS_TABLE}
     where {STORE_SALES_TABLE}.ss_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
       and {STORE_SALES_TABLE}.ss_store_sk = {STORE_TABLE}.s_store_sk
       and {STORE_SALES_TABLE}.ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and {STORE_SALES_TABLE}.ss_addr_sk = {CUSTOMER_ADDRESS_TABLE}.ca_address_sk
       and ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 4 or
            {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count= 3)
       and {DATE_DIM_TABLE}.d_dow in (6,0)
       and {DATE_DIM_TABLE}.d_year in (1999,1999+1,1999+2)
       and {STORE_TABLE}.s_city in ('Woodland','Rockville','Oakdale','Pleasant Grove','Springfield')
     group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,{CUSTOMER_TABLE},{CUSTOMER_ADDRESS_TABLE} current_addr
where ss_customer_sk = c_customer_sk
  and {CUSTOMER_TABLE}.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> bought_city
order by c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
    limit 100
                        """
    ),
    (
        "query47",
        """


        with v1 as(
    select i_category, i_brand,
           s_store_name, s_company_name,
           d_year, d_moy,
           sum(ss_sales_price) sum_sales,
           avg(sum(ss_sales_price)) over
        (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
            rank() over
        (partition by i_category, i_brand,
                     s_store_name, s_company_name
           order by d_year, d_moy) rn
    from {ITEM_TABLE}, {STORE_SALES_TABLE}, {DATE_DIM_TABLE}, {STORE_TABLE}
    where ss_item_sk = i_item_sk and
            ss_sold_date_sk = d_date_sk and
            ss_store_sk = s_store_sk and
        (
                    d_year = 1999 or
                    ( d_year = 1999-1 and d_moy =12) or
                    ( d_year = 1999+1 and d_moy =1)
            )
    group by i_category, i_brand,
             s_store_name, s_company_name,
             d_year, d_moy),
                                                                 v2 as(
                                                                     select v1.s_store_name, v1.s_company_name
                                                                          ,v1.d_year, v1.d_moy
                                                                          ,v1.avg_monthly_sales
                                                                          ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
                                                                     from v1, v1 v1_lag, v1 v1_lead
                                                                     where v1.i_category = v1_lag.i_category and
                                                                             v1.i_category = v1_lead.i_category and
                                                                             v1.i_brand = v1_lag.i_brand and
                                                                             v1.i_brand = v1_lead.i_brand and
                                                                             v1.s_store_name = v1_lag.s_store_name and
                                                                             v1.s_store_name = v1_lead.s_store_name and
                                                                             v1.s_company_name = v1_lag.s_company_name and
                                                                             v1.s_company_name = v1_lead.s_company_name and
                                                                             v1.rn = v1_lag.rn + 1 and
                                                                             v1.rn = v1_lead.rn - 1)
select  *
from v2
where  d_year = 1999 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by sum_sales - avg_monthly_sales, sum_sales
    limit 100
                        """
    ),
    (
        "query48",
        """


        select sum (ss_quantity)
from {STORE_SALES_TABLE}, {STORE_TABLE}, {CUSTOMER_DEMOGRAPHICS_TABLE}, {CUSTOMER_ADDRESS_TABLE}, {DATE_DIM_TABLE}
where s_store_sk = ss_store_sk
  and  ss_sold_date_sk = d_date_sk and d_year = 2001
  and
    (
            (
                        cd_demo_sk = ss_cdemo_sk
                    and
                        cd_marital_status = 'D'
                    and
                        cd_education_status = 'Secondary'
                    and
                        ss_sales_price between 100.00 and 150.00
                )
            or
            (
                        cd_demo_sk = ss_cdemo_sk
                    and
                        cd_marital_status = 'W'
                    and
                        cd_education_status = '2 yr Degree'
                    and
                        ss_sales_price between 50.00 and 100.00
                )
            or
            (
                        cd_demo_sk = ss_cdemo_sk
                    and
                        cd_marital_status = 'U'
                    and
                        cd_education_status = 'Unknown'
                    and
                        ss_sales_price between 150.00 and 200.00
                )
        )
  and
    (
            (
                        ss_addr_sk = ca_address_sk
                    and
                        ca_country = 'United States'
                    and
                        ca_state in ('VA', 'MI', 'FL')
                    and ss_net_profit between 0 and 2000
                )
            or
            (ss_addr_sk = ca_address_sk
                and
             ca_country = 'United States'
                and
             ca_state in ('SC', 'GA', 'MN')
                and ss_net_profit between 150 and 3000
                )
            or
            (ss_addr_sk = ca_address_sk
                and
             ca_country = 'United States'
                and
             ca_state in ('OK', 'IA', 'TX')
                and ss_net_profit between 50 and 25000
                )
        )
                        """
    ),
    (
        "query49",
        """


        select channel, {ITEM_TABLE}, return_ratio, return_rank, currency_rank from
    (select
         'web' as channel
          ,web.{ITEM_TABLE}
          ,web.return_ratio
          ,web.return_rank
          ,web.currency_rank
     from (
              select
                  {ITEM_TABLE}
                   ,return_ratio
                   ,currency_ratio
                   ,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
              from
                  (	select ws.ws_item_sk as {ITEM_TABLE}
                            ,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/
                              cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
                            ,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/
                              cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
                       from
                           {WEB_SALES_TABLE} ws left outer join {WEB_RETURNS_TABLE} wr
                                                        on (ws.ws_order_number = wr.wr_order_number and
                                                            ws.ws_item_sk = wr.wr_item_sk)
                          ,{DATE_DIM_TABLE}
                       where
                               wr.wr_return_amt > 10000
                         and ws.ws_net_profit > 1
                         and ws.ws_net_paid > 0
                         and ws.ws_quantity > 0
                         and ws_sold_date_sk = d_date_sk
                         and d_year = 2002
                         and d_moy = 11
                       group by ws.ws_item_sk
                  ) in_web
          ) web
     where
         (
                     web.return_rank <= 10
                 or
                     web.currency_rank <= 10
             )
     union
     select
         'catalog' as channel
          ,catalog.{ITEM_TABLE}
          ,catalog.return_ratio
          ,catalog.return_rank
          ,catalog.currency_rank
     from (
              select
                  {ITEM_TABLE}
                   ,return_ratio
                   ,currency_ratio
                   ,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
              from
                  (	select
                           cs.cs_item_sk as {ITEM_TABLE}
                            ,(cast(sum(coalesce(cr.cr_return_quantity,0)) as decimal(15,4))/
                              cast(sum(coalesce(cs.cs_quantity,0)) as decimal(15,4) )) as return_ratio
                            ,(cast(sum(coalesce(cr.cr_return_amount,0)) as decimal(15,4))/
                              cast(sum(coalesce(cs.cs_net_paid,0)) as decimal(15,4) )) as currency_ratio
                       from
                           {CATALOG_SALES_TABLE} cs left outer join {CATALOG_RETURNS_TABLE} cr
                                                            on (cs.cs_order_number = cr.cr_order_number and
                                                                cs.cs_item_sk = cr.cr_item_sk)
                          ,{DATE_DIM_TABLE}
                       where
                               cr.cr_return_amount > 10000
                         and cs.cs_net_profit > 1
                         and cs.cs_net_paid > 0
                         and cs.cs_quantity > 0
                         and cs_sold_date_sk = d_date_sk
                         and d_year = 2002
                         and d_moy = 11
                       group by cs.cs_item_sk
                  ) in_cat
          ) catalog
     where
         (
         catalog.return_rank <= 10
        or
         catalog.currency_rank <=10
         )
     union
     select
         '{STORE_TABLE}' as channel
             ,{STORE_TABLE}.{ITEM_TABLE}
             ,{STORE_TABLE}.return_ratio
             ,{STORE_TABLE}.return_rank
             ,{STORE_TABLE}.currency_rank
     from (
         select
         {ITEM_TABLE}
             ,return_ratio
             ,currency_ratio
             ,rank() over (order by return_ratio) as return_rank
             ,rank() over (order by currency_ratio) as currency_rank
         from
         (	select sts.ss_item_sk as {ITEM_TABLE}
             ,(cast(sum(coalesce(sr.sr_return_quantity,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) as decimal(15,4) )) as return_ratio
             ,(cast(sum(coalesce(sr.sr_return_amt,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) as decimal(15,4) )) as currency_ratio
         from
         {STORE_SALES_TABLE} sts left outer join {STORE_RETURNS_TABLE} sr
         on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk)
             ,{DATE_DIM_TABLE}
         where
         sr.sr_return_amt > 10000
         and sts.ss_net_profit > 1
         and sts.ss_net_paid > 0
         and sts.ss_quantity > 0
         and ss_sold_date_sk = d_date_sk
         and d_year = 2002
         and d_moy = 11
         group by sts.ss_item_sk
         ) in_store
         ) {STORE_TABLE}
     where  (
         {STORE_TABLE}.return_rank <= 10
        or
         {STORE_TABLE}.currency_rank <= 10
         )
    )
order by 1,4,5,2
    limit 100
                        """
    ),
    (
        "query50",
        """


        select s_store_name
     ,s_company_id
     ,s_street_number
     ,s_street_name
     ,s_street_type
     ,s_suite_number
     ,s_city
     ,s_county
     ,s_state
     ,s_zip
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and
                    (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and
                    (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
                    (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
     ,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
from
    {STORE_SALES_TABLE}
   ,{STORE_RETURNS_TABLE}
   ,{STORE_TABLE}
   ,{DATE_DIM_TABLE} d1
   ,{DATE_DIM_TABLE} d2
where
        d2.d_year = 1998
  and d2.d_moy  = 10
  and ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_sold_date_sk   = d1.d_date_sk
  and sr_returned_date_sk   = d2.d_date_sk
  and ss_customer_sk = sr_customer_sk
  and ss_store_sk = s_store_sk
group by
    s_store_name
       ,s_company_id
       ,s_street_number
       ,s_street_name
       ,s_street_type
       ,s_suite_number
       ,s_city
       ,s_county
       ,s_state
       ,s_zip
order by s_store_name
       ,s_company_id
       ,s_street_number
       ,s_street_name
       ,s_street_type
       ,s_suite_number
       ,s_city
       ,s_county
       ,s_state
       ,s_zip
    limit 100
                        """
    ),
    (
        "query51",
        """


        WITH web_v1 as (
    select
        ws_item_sk item_sk, d_date,
        sum(sum(ws_sales_price))
                   over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
    from {WEB_SALES_TABLE}
       ,{DATE_DIM_TABLE}
    where ws_sold_date_sk=d_date_sk
      and d_month_seq between 1176 and 1176+11
      and ws_item_sk is not NULL
    group by ws_item_sk, d_date),
                                                                 store_v1 as (
                                                                     select
                                                                         ss_item_sk item_sk, d_date,
                                                                         sum(sum(ss_sales_price))
                                                                                    over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
                                                                     from {STORE_SALES_TABLE}
                                                                        ,{DATE_DIM_TABLE}
                                                                     where ss_sold_date_sk=d_date_sk
                                                                       and d_month_seq between 1176 and 1176+11
                                                                       and ss_item_sk is not NULL
                                                                     group by ss_item_sk, d_date)
select  *
from (select item_sk
           ,d_date
           ,{WEB_SALES_TABLE}
           ,{STORE_SALES_TABLE}
           ,max({WEB_SALES_TABLE})
        over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative
     ,max({STORE_SALES_TABLE})
            over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative
      from (select case when web.item_sk is not null then web.item_sk else {STORE_TABLE}.item_sk end item_sk
                 ,case when web.d_date is not null then web.d_date else {STORE_TABLE}.d_date end d_date
                 ,web.cume_sales {WEB_SALES_TABLE}
                 ,{STORE_TABLE}.cume_sales {STORE_SALES_TABLE}
            from web_v1 web full outer join store_v1 {STORE_TABLE} on (web.item_sk = {STORE_TABLE}.item_sk
                and web.d_date = {STORE_TABLE}.d_date)
           )x )y
where web_cumulative > store_cumulative
order by item_sk
       ,d_date
    limit 100
                        """
    ),
    (
        "query52",
        """


        select dt.d_year
     ,{ITEM_TABLE}.i_brand_id brand_id
     ,{ITEM_TABLE}.i_brand brand
     ,sum(ss_ext_sales_price) ext_price
from {DATE_DIM_TABLE} dt
   ,{STORE_SALES_TABLE}
   ,{ITEM_TABLE}
where dt.d_date_sk = {STORE_SALES_TABLE}.ss_sold_date_sk
  and {STORE_SALES_TABLE}.ss_item_sk = {ITEM_TABLE}.i_item_sk
  and {ITEM_TABLE}.i_manager_id = 1
  and dt.d_moy=11
  and dt.d_year=2002
group by dt.d_year
       ,{ITEM_TABLE}.i_brand
       ,{ITEM_TABLE}.i_brand_id
order by dt.d_year
       ,ext_price desc
       ,brand_id
    limit 100
                        """
    ),
    (
        "query53",
        """


        select * from
    (select i_manufact_id,
            sum(ss_sales_price) sum_sales,
            avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
     from {ITEM_TABLE}, {STORE_SALES_TABLE}, {DATE_DIM_TABLE}, {STORE_TABLE}
     where ss_item_sk = i_item_sk and
             ss_sold_date_sk = d_date_sk and
             ss_store_sk = s_store_sk and
             d_month_seq in (1213,1213+1,1213+2,1213+3,1213+4,1213+5,1213+6,1213+7,1213+8,1213+9,1213+10,1213+11) and
         ((i_category in ('Books','Children','Electronics') and
           i_class in ('personal','portable','reference','self-help') and
           i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
                       'exportiunivamalg #9','scholaramalgamalg #9'))
             or(i_category in ('Women','Music','Men') and
                i_class in ('accessories','classical','fragrances','pants') and
                i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
                            'importoamalg #1')))
     group by i_manufact_id, d_qoy ) tmp1
where case when avg_quarterly_sales > 0
               then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales
           else null end > 0.1
order by avg_quarterly_sales,
         sum_sales,
         i_manufact_id
    limit 100
                        """
    ),
    (
        "query54",
        """


        with my_customers as (
    select distinct c_customer_sk
                  , c_current_addr_sk
    from
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   {CATALOG_SALES_TABLE}
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   {WEB_SALES_TABLE}
        ) cs_or_ws_sales,
        {ITEM_TABLE},
        {DATE_DIM_TABLE},
        {CUSTOMER_TABLE}
    where   sold_date_sk = d_date_sk
      and item_sk = i_item_sk
      and i_category = 'Shoes'
      and i_class = 'womens'
      and c_customer_sk = cs_or_ws_sales.customer_sk
      and d_moy = 5
      and d_year = 2000
)
   , my_revenue as (
    select c_customer_sk,
           sum(ss_ext_sales_price) as revenue
    from   my_customers,
           {STORE_SALES_TABLE},
           {CUSTOMER_ADDRESS_TABLE},
           {STORE_TABLE},
           {DATE_DIM_TABLE}
    where  c_current_addr_sk = ca_address_sk
      and ca_county = s_county
      and ca_state = s_state
      and ss_sold_date_sk = d_date_sk
      and c_customer_sk = ss_customer_sk
      and d_month_seq between (select distinct d_month_seq+1
                               from   {DATE_DIM_TABLE} where d_year = 2000 and d_moy = 5)
        and  (select distinct d_month_seq+3
              from   {DATE_DIM_TABLE} where d_year = 2000 and d_moy = 5)
    group by c_customer_sk
)
   , segments as
    (select cast((revenue/50) as int) as segment
     from   my_revenue
    )
select  segment, count(*) as num_customers, segment*50 as segment_base
from segments
group by segment
order by segment, num_customers
    limit 100
                        """
    ),
    (
        "query55",
        """


        select i_brand_id brand_id, i_brand brand,
                                                                    sum(ss_ext_sales_price) ext_price
from {DATE_DIM_TABLE}, {STORE_SALES_TABLE}, {ITEM_TABLE}
where d_date_sk = ss_sold_date_sk
  and ss_item_sk = i_item_sk
  and i_manager_id=96
  and d_moy=12
  and d_year=2002
group by i_brand, i_brand_id
order by ext_price desc, i_brand_id
    limit 100
                        """
    ),
    (
        "query56",
        """


        with ss as (
    select i_item_id,sum(ss_ext_sales_price) total_sales
    from
        {STORE_SALES_TABLE},
        {DATE_DIM_TABLE},
        {CUSTOMER_ADDRESS_TABLE},
        {ITEM_TABLE}
    where i_item_id in (select
                            i_item_id
                        from {ITEM_TABLE}
                        where i_color in ('rose','brown','drab'))
      and     ss_item_sk              = i_item_sk
      and     ss_sold_date_sk         = d_date_sk
      and     d_year                  = 2000
      and     d_moy                   = 4
      and     ss_addr_sk              = ca_address_sk
      and     ca_gmt_offset           = -5
    group by i_item_id),
                                                                 cs as (
                                                                     select i_item_id,sum(cs_ext_sales_price) total_sales
                                                                     from
                                                                         {CATALOG_SALES_TABLE},
                                                                         {DATE_DIM_TABLE},
                                                                         {CUSTOMER_ADDRESS_TABLE},
                                                                         {ITEM_TABLE}
                                                                     where
                                                                             i_item_id               in (select
                                                                                                             i_item_id
                                                                                                         from {ITEM_TABLE}
                                                                                                         where i_color in ('rose','brown','drab'))
                                                                       and     cs_item_sk              = i_item_sk
                                                                       and     cs_sold_date_sk         = d_date_sk
                                                                       and     d_year                  = 2000
                                                                       and     d_moy                   = 4
                                                                       and     cs_bill_addr_sk         = ca_address_sk
                                                                       and     ca_gmt_offset           = -5
                                                                     group by i_item_id),
                                                                 ws as (
                                                                     select i_item_id,sum(ws_ext_sales_price) total_sales
                                                                     from
                                                                         {WEB_SALES_TABLE},
                                                                         {DATE_DIM_TABLE},
                                                                         {CUSTOMER_ADDRESS_TABLE},
                                                                         {ITEM_TABLE}
                                                                     where
                                                                             i_item_id               in (select
                                                                                                             i_item_id
                                                                                                         from {ITEM_TABLE}
                                                                                                         where i_color in ('rose','brown','drab'))
                                                                       and     ws_item_sk              = i_item_sk
                                                                       and     ws_sold_date_sk         = d_date_sk
                                                                       and     d_year                  = 2000
                                                                       and     d_moy                   = 4
                                                                       and     ws_bill_addr_sk         = ca_address_sk
                                                                       and     ca_gmt_offset           = -5
                                                                     group by i_item_id)
select  i_item_id ,sum(total_sales) total_sales
from  (select * from ss
       union all
       select * from cs
       union all
       select * from ws) tmp1
group by i_item_id
order by total_sales,
         i_item_id
    limit 100
                        """
    ),
    (
        "query57",
        """


        with v1 as(
    select i_category, i_brand,
           cc_name,
           d_year, d_moy,
           sum(cs_sales_price) sum_sales,
           avg(sum(cs_sales_price)) over
        (partition by i_category, i_brand,
                     cc_name, d_year)
          avg_monthly_sales,
            rank() over
        (partition by i_category, i_brand,
                     cc_name
           order by d_year, d_moy) rn
    from {ITEM_TABLE}, {CATALOG_SALES_TABLE}, {DATE_DIM_TABLE}, {CALL_CENTER_TABLE}
    where cs_item_sk = i_item_sk and
            cs_sold_date_sk = d_date_sk and
            cc_call_center_sk= cs_call_center_sk and
        (
                    d_year = 2001 or
                    ( d_year = 2001-1 and d_moy =12) or
                    ( d_year = 2001+1 and d_moy =1)
            )
    group by i_category, i_brand,
             cc_name , d_year, d_moy),
                                                                 v2 as(
                                                                     select v1.i_brand
                                                                          ,v1.d_year, v1.d_moy
                                                                          ,v1.avg_monthly_sales
                                                                          ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
                                                                     from v1, v1 v1_lag, v1 v1_lead
                                                                     where v1.i_category = v1_lag.i_category and
                                                                             v1.i_category = v1_lead.i_category and
                                                                             v1.i_brand = v1_lag.i_brand and
                                                                             v1.i_brand = v1_lead.i_brand and
                                                                             v1. cc_name = v1_lag. cc_name and
                                                                             v1. cc_name = v1_lead. cc_name and
                                                                             v1.rn = v1_lag.rn + 1 and
                                                                             v1.rn = v1_lead.rn - 1)
select  *
from v2
where  d_year = 2001 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by sum_sales - avg_monthly_sales, nsum
    limit 100
                        """
    ),
    (
        "query58",
        """


        with ss_items as
                                                                     (select i_item_id item_id
                                                                           ,sum(ss_ext_sales_price) ss_item_rev
                                                                      from {STORE_SALES_TABLE}
                                                                         ,{ITEM_TABLE}
                                                                         ,{DATE_DIM_TABLE}
                                                                      where ss_item_sk = i_item_sk
                                                                        and d_date in (select d_date
                                                                                       from {DATE_DIM_TABLE}
                                                                                       where d_week_seq = (select d_week_seq
                                                                                                           from {DATE_DIM_TABLE}
                                                                                                           where d_date = '1999-04-16'))
                                                                        and ss_sold_date_sk   = d_date_sk
                                                                      group by i_item_id),
                                                                 cs_items as
                                                                     (select i_item_id item_id
                                                                           ,sum(cs_ext_sales_price) cs_item_rev
                                                                      from {CATALOG_SALES_TABLE}
                                                                         ,{ITEM_TABLE}
                                                                         ,{DATE_DIM_TABLE}
                                                                      where cs_item_sk = i_item_sk
                                                                        and  d_date in (select d_date
                                                                                        from {DATE_DIM_TABLE}
                                                                                        where d_week_seq = (select d_week_seq
                                                                                                            from {DATE_DIM_TABLE}
                                                                                                            where d_date = '1999-04-16'))
                                                                        and  cs_sold_date_sk = d_date_sk
                                                                      group by i_item_id),
                                                                 ws_items as
                                                                     (select i_item_id item_id
                                                                           ,sum(ws_ext_sales_price) ws_item_rev
                                                                      from {WEB_SALES_TABLE}
                                                                         ,{ITEM_TABLE}
                                                                         ,{DATE_DIM_TABLE}
                                                                      where ws_item_sk = i_item_sk
                                                                        and  d_date in (select d_date
                                                                                        from {DATE_DIM_TABLE}
                                                                                        where d_week_seq =(select d_week_seq
                                                                                                           from {DATE_DIM_TABLE}
                                                                                                           where d_date = '1999-04-16'))
                                                                        and ws_sold_date_sk   = d_date_sk
                                                                      group by i_item_id)
select  ss_items.item_id
     ,ss_item_rev
     ,ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev
     ,cs_item_rev
     ,cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev
     ,ws_item_rev
     ,ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev
     ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
from ss_items,cs_items,ws_items
where ss_items.item_id=cs_items.item_id
  and ss_items.item_id=ws_items.item_id
  and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
  and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
  and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
  and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
  and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
  and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
order by item_id
       ,ss_item_rev
    limit 100
                        """
    ),
    (
        "query59",
        """


        with wss as
                                                                     (select d_week_seq,
                                                                             ss_store_sk,
                                                                             sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
                                                                             sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
                                                                             sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
                                                                             sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
                                                                             sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
                                                                             sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
                                                                             sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
                                                                      from {STORE_SALES_TABLE},{DATE_DIM_TABLE}
                                                                      where d_date_sk = ss_sold_date_sk
                                                                      group by d_week_seq,ss_store_sk
                                                                     )
select  s_store_name1,s_store_id1,d_week_seq1
     ,sun_sales1/sun_sales2,mon_sales1/mon_sales2
     ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2
     ,fri_sales1/fri_sales2,sat_sales1/sat_sales2
from
    (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1
          ,s_store_id s_store_id1,sun_sales sun_sales1
          ,mon_sales mon_sales1,tue_sales tue_sales1
          ,wed_sales wed_sales1,thu_sales thu_sales1
          ,fri_sales fri_sales1,sat_sales sat_sales1
     from wss,{STORE_TABLE},{DATE_DIM_TABLE} d
     where d.d_week_seq = wss.d_week_seq and
             ss_store_sk = s_store_sk and
         d_month_seq between 1201 and 1201 + 11) y,
    (select s_store_name s_store_name2,wss.d_week_seq d_week_seq2
          ,s_store_id s_store_id2,sun_sales sun_sales2
          ,mon_sales mon_sales2,tue_sales tue_sales2
          ,wed_sales wed_sales2,thu_sales thu_sales2
          ,fri_sales fri_sales2,sat_sales sat_sales2
     from wss,{STORE_TABLE},{DATE_DIM_TABLE} d
     where d.d_week_seq = wss.d_week_seq and
             ss_store_sk = s_store_sk and
         d_month_seq between 1201+ 12 and 1201 + 23) x
where s_store_id1=s_store_id2
  and d_week_seq1=d_week_seq2-52
order by s_store_name1,s_store_id1,d_week_seq1
    limit 100
                        """
    ),
    (
        "query60",
        """


        with ss as (
    select
        i_item_id,sum(ss_ext_sales_price) total_sales
    from
        {STORE_SALES_TABLE},
        {DATE_DIM_TABLE},
        {CUSTOMER_ADDRESS_TABLE},
        {ITEM_TABLE}
    where
            i_item_id in (select
                              i_item_id
                          from
                              {ITEM_TABLE}
                          where i_category in ('Jewelry'))
      and     ss_item_sk              = i_item_sk
      and     ss_sold_date_sk         = d_date_sk
      and     d_year                  = 1999
      and     d_moy                   = 9
      and     ss_addr_sk              = ca_address_sk
      and     ca_gmt_offset           = -6
    group by i_item_id),
                                                                 cs as (
                                                                     select
                                                                         i_item_id,sum(cs_ext_sales_price) total_sales
                                                                     from
                                                                         {CATALOG_SALES_TABLE},
                                                                         {DATE_DIM_TABLE},
                                                                         {CUSTOMER_ADDRESS_TABLE},
                                                                         {ITEM_TABLE}
                                                                     where
                                                                             i_item_id               in (select
                                                                                                             i_item_id
                                                                                                         from
                                                                                                             {ITEM_TABLE}
                                                                                                         where i_category in ('Jewelry'))
                                                                       and     cs_item_sk              = i_item_sk
                                                                       and     cs_sold_date_sk         = d_date_sk
                                                                       and     d_year                  = 1999
                                                                       and     d_moy                   = 9
                                                                       and     cs_bill_addr_sk         = ca_address_sk
                                                                       and     ca_gmt_offset           = -6
                                                                     group by i_item_id),
                                                                 ws as (
                                                                     select
                                                                         i_item_id,sum(ws_ext_sales_price) total_sales
                                                                     from
                                                                         {WEB_SALES_TABLE},
                                                                         {DATE_DIM_TABLE},
                                                                         {CUSTOMER_ADDRESS_TABLE},
                                                                         {ITEM_TABLE}
                                                                     where
                                                                             i_item_id               in (select
                                                                                                             i_item_id
                                                                                                         from
                                                                                                             {ITEM_TABLE}
                                                                                                         where i_category in ('Jewelry'))
                                                                       and     ws_item_sk              = i_item_sk
                                                                       and     ws_sold_date_sk         = d_date_sk
                                                                       and     d_year                  = 1999
                                                                       and     d_moy                   = 9
                                                                       and     ws_bill_addr_sk         = ca_address_sk
                                                                       and     ca_gmt_offset           = -6
                                                                     group by i_item_id)
select
    i_item_id
     ,sum(total_sales) total_sales
from  (select * from ss
       union all
       select * from cs
       union all
       select * from ws) tmp1
group by i_item_id
order by i_item_id
       ,total_sales
    limit 100
                        """
    ),
    (
        "query61",
        """


        select promotions,total,cast(promotions as decimal(17,4))/cast(total as decimal(17,4))*100
from
    (select sum(ss_ext_sales_price) promotions
     from  {STORE_SALES_TABLE}
        ,{STORE_TABLE}
        ,{PROMOTION_TABLE}
        ,{DATE_DIM_TABLE}
        ,{CUSTOMER_TABLE}
        ,{CUSTOMER_ADDRESS_TABLE}
        ,{ITEM_TABLE}
     where ss_sold_date_sk = d_date_sk
       and   ss_store_sk = s_store_sk
       and   ss_promo_sk = p_promo_sk
       and   ss_customer_sk= c_customer_sk
       and   ca_address_sk = c_current_addr_sk
       and   ss_item_sk = i_item_sk
       and   ca_gmt_offset = -6
       and   i_category = 'Sports'
       and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
       and   s_gmt_offset = -6
       and   d_year = 1999
       and   d_moy  = 11) promotional_sales,
    (select sum(ss_ext_sales_price) total
     from  {STORE_SALES_TABLE}
        ,{STORE_TABLE}
        ,{DATE_DIM_TABLE}
        ,{CUSTOMER_TABLE}
        ,{CUSTOMER_ADDRESS_TABLE}
        ,{ITEM_TABLE}
     where ss_sold_date_sk = d_date_sk
       and   ss_store_sk = s_store_sk
       and   ss_customer_sk= c_customer_sk
       and   ca_address_sk = c_current_addr_sk
       and   ss_item_sk = i_item_sk
       and   ca_gmt_offset = -6
       and   i_category = 'Sports'
       and   s_gmt_offset = -6
       and   d_year = 1999
       and   d_moy  = 11) all_sales
order by promotions, total
    limit 100
                        """
    ),
    (
        "query62",
        """


        select substr(w_warehouse_name,1,20)
     ,sm_type
     ,web_name
     ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
     ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and
                    (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
     ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and
                    (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
     ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
                    (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
     ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
from
    {WEB_SALES_TABLE}
   ,{WAREHOUSE_TABLE}
   ,{SHIP_MODE_TABLE}
   ,{WEB_SITE_TABLE}
   ,{DATE_DIM_TABLE}
where
    d_month_seq between 1214 and 1214 + 11
  and ws_ship_date_sk   = d_date_sk
  and ws_warehouse_sk   = w_warehouse_sk
  and ws_ship_mode_sk   = sm_ship_mode_sk
  and ws_web_site_sk    = web_site_sk
group by
    substr(w_warehouse_name,1,20)
       ,sm_type
       ,web_name
order by substr(w_warehouse_name,1,20)
       ,sm_type
       ,web_name
    limit 100
                        """
    ),
    (
        "query63",
        """


        select *
from (select i_manager_id
           ,sum(ss_sales_price) sum_sales
           ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
      from {ITEM_TABLE}
         ,{STORE_SALES_TABLE}
         ,{DATE_DIM_TABLE}
         ,{STORE_TABLE}
      where ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and ss_store_sk = s_store_sk
        and d_month_seq in (1188,1188+1,1188+2,1188+3,1188+4,1188+5,1188+6,1188+7,1188+8,1188+9,1188+10,1188+11)
        and ((    i_category in ('Books','Children','Electronics')
          and i_class in ('personal','portable','reference','self-help')
          and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
                          'exportiunivamalg #9','scholaramalgamalg #9'))
          or(    i_category in ('Women','Music','Men')
              and i_class in ('accessories','classical','fragrances','pants')
              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
                              'importoamalg #1')))
      group by i_manager_id, d_moy) tmp1
where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by i_manager_id
       ,avg_monthly_sales
       ,sum_sales
    limit 100
                        """
    ),
    (
        "query64",
        """


        with cs_ui as
                                                                     (select cs_item_sk
                                                                           ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
                                                                      from {CATALOG_SALES_TABLE}
                                                                         ,{CATALOG_RETURNS_TABLE}
                                                                      where cs_item_sk = cr_item_sk
                                                                        and cs_order_number = cr_order_number
                                                                      group by cs_item_sk
                                                                      having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
                                                                 cross_sales as
                                                                     (select i_product_name product_name
                                                                           ,i_item_sk item_sk
                                                                           ,s_store_name store_name
                                                                           ,s_zip store_zip
                                                                           ,ad1.ca_street_number b_street_number
                                                                           ,ad1.ca_street_name b_street_name
                                                                           ,ad1.ca_city b_city
                                                                           ,ad1.ca_zip b_zip
                                                                           ,ad2.ca_street_number c_street_number
                                                                           ,ad2.ca_street_name c_street_name
                                                                           ,ad2.ca_city c_city
                                                                           ,ad2.ca_zip c_zip
                                                                           ,d1.d_year as syear
                                                                           ,d2.d_year as fsyear
                                                                           ,d3.d_year s2year
                                                                           ,count(*) cnt
                                                                           ,sum(ss_wholesale_cost) s1
                                                                           ,sum(ss_list_price) s2
                                                                           ,sum(ss_coupon_amt) s3
                                                                      FROM   {STORE_SALES_TABLE}
                                                                         ,{STORE_RETURNS_TABLE}
                                                                         ,cs_ui
                                                                         ,{DATE_DIM_TABLE} d1
                                                                         ,{DATE_DIM_TABLE} d2
                                                                         ,{DATE_DIM_TABLE} d3
                                                                         ,{STORE_TABLE}
                                                                         ,{CUSTOMER_TABLE}
                                                                         ,{CUSTOMER_DEMOGRAPHICS_TABLE} cd1
                                                                         ,{CUSTOMER_DEMOGRAPHICS_TABLE} cd2
                                                                         ,{PROMOTION_TABLE}
                                                                         ,{HOUSEHOLD_DEMOGRAPHICS_TABLE} hd1
                                                                         ,{HOUSEHOLD_DEMOGRAPHICS_TABLE} hd2
                                                                         ,{CUSTOMER_ADDRESS_TABLE} ad1
                                                                         ,{CUSTOMER_ADDRESS_TABLE} ad2
                                                                         ,{INCOME_BAND_TABLE} ib1
                                                                         ,{INCOME_BAND_TABLE} ib2
                                                                         ,{ITEM_TABLE}
                                                                      WHERE  ss_store_sk = s_store_sk AND
                                                                              ss_sold_date_sk = d1.d_date_sk AND
                                                                              ss_customer_sk = c_customer_sk AND
                                                                              ss_cdemo_sk= cd1.cd_demo_sk AND
                                                                              ss_hdemo_sk = hd1.hd_demo_sk AND
                                                                              ss_addr_sk = ad1.ca_address_sk and
                                                                              ss_item_sk = i_item_sk and
                                                                              ss_item_sk = sr_item_sk and
                                                                              ss_ticket_number = sr_ticket_number and
                                                                              ss_item_sk = cs_ui.cs_item_sk and
                                                                              c_current_cdemo_sk = cd2.cd_demo_sk AND
                                                                              c_current_hdemo_sk = hd2.hd_demo_sk AND
                                                                              c_current_addr_sk = ad2.ca_address_sk and
                                                                              c_first_sales_date_sk = d2.d_date_sk and
                                                                              c_first_shipto_date_sk = d3.d_date_sk and
                                                                              ss_promo_sk = p_promo_sk and
                                                                              hd1.hd_income_band_sk = ib1.ib_income_band_sk and
                                                                              hd2.hd_income_band_sk = ib2.ib_income_band_sk and
                                                                              cd1.cd_marital_status <> cd2.cd_marital_status and
                                                                              i_color in ('burlywood','coral','dark','puff','smoke','peach') and
                                                                          i_current_price between 22 and 22 + 10 and
                                                                          i_current_price between 22 + 1 and 22 + 15
                                                                      group by i_product_name
                                                                             ,i_item_sk
                                                                             ,s_store_name
                                                                             ,s_zip
                                                                             ,ad1.ca_street_number
                                                                             ,ad1.ca_street_name
                                                                             ,ad1.ca_city
                                                                             ,ad1.ca_zip
                                                                             ,ad2.ca_street_number
                                                                             ,ad2.ca_street_name
                                                                             ,ad2.ca_city
                                                                             ,ad2.ca_zip
                                                                             ,d1.d_year
                                                                             ,d2.d_year
                                                                             ,d3.d_year
                                                                     )
select cs1.product_name
     ,cs1.store_name
     ,cs1.store_zip
     ,cs1.b_street_number
     ,cs1.b_street_name
     ,cs1.b_city
     ,cs1.b_zip
     ,cs1.c_street_number
     ,cs1.c_street_name
     ,cs1.c_city
     ,cs1.c_zip
     ,cs1.syear
     ,cs1.cnt
     ,cs1.s1 as s11
     ,cs1.s2 as s21
     ,cs1.s3 as s31
     ,cs2.s1 as s12
     ,cs2.s2 as s22
     ,cs2.s3 as s32
     ,cs2.syear
     ,cs2.cnt
from cross_sales cs1,cross_sales cs2
where cs1.item_sk=cs2.item_sk and
        cs1.syear = 1999 and
        cs2.syear = 1999 + 1 and
        cs2.cnt <= cs1.cnt and
        cs1.store_name = cs2.store_name and
        cs1.store_zip = cs2.store_zip
order by cs1.product_name
       ,cs1.store_name
       ,cs2.cnt
       ,cs1.s1
       ,cs2.s1
                        """
    ),
    (
        "query65",
        """


        select s_store_name,
    i_item_desc,
    sc.revenue,
    i_current_price,
    i_wholesale_cost,
    i_brand
from {STORE_TABLE}, {ITEM_TABLE},
     (select ss_store_sk, avg(revenue) as ave
      from
          (select  ss_store_sk, ss_item_sk,
                   sum(ss_sales_price) as revenue
           from {STORE_SALES_TABLE}, {DATE_DIM_TABLE}
           where ss_sold_date_sk = d_date_sk and d_month_seq between 1192 and 1192+11
           group by ss_store_sk, ss_item_sk) sa
      group by ss_store_sk) sb,
     (select  ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
      from {STORE_SALES_TABLE}, {DATE_DIM_TABLE}
      where ss_sold_date_sk = d_date_sk and d_month_seq between 1192 and 1192+11
      group by ss_store_sk, ss_item_sk) sc
where sb.ss_store_sk = sc.ss_store_sk and
        sc.revenue <= 0.1 * sb.ave and
        s_store_sk = sc.ss_store_sk and
        i_item_sk = sc.ss_item_sk
order by s_store_name, i_item_desc
    limit 100
                        """
    ),
    (
        "query66",
        """


        select w_warehouse_name
     ,w_warehouse_sq_ft
     ,w_city
     ,w_county
     ,w_state
     ,w_country
     ,ship_carriers
     ,year
     ,sum(jan_sales) as jan_sales
     ,sum(feb_sales) as feb_sales
     ,sum(mar_sales) as mar_sales
     ,sum(apr_sales) as apr_sales
     ,sum(may_sales) as may_sales
     ,sum(jun_sales) as jun_sales
     ,sum(jul_sales) as jul_sales
     ,sum(aug_sales) as aug_sales
     ,sum(sep_sales) as sep_sales
     ,sum(oct_sales) as oct_sales
     ,sum(nov_sales) as nov_sales
     ,sum(dec_sales) as dec_sales
     ,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
     ,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
     ,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
     ,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
     ,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
     ,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
     ,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
     ,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
     ,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
     ,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
     ,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
     ,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
     ,sum(jan_net) as jan_net
     ,sum(feb_net) as feb_net
     ,sum(mar_net) as mar_net
     ,sum(apr_net) as apr_net
     ,sum(may_net) as may_net
     ,sum(jun_net) as jun_net
     ,sum(jul_net) as jul_net
     ,sum(aug_net) as aug_net
     ,sum(sep_net) as sep_net
     ,sum(oct_net) as oct_net
     ,sum(nov_net) as nov_net
     ,sum(dec_net) as dec_net
from (
    select
    w_warehouse_name
        ,w_warehouse_sq_ft
        ,w_city
        ,w_county
        ,w_state
        ,w_country
        ,'GREAT EASTERN' || ',' || 'UPS' as ship_carriers
        ,d_year as year
        ,sum(case when d_moy = 1
    then ws_ext_sales_price* ws_quantity else 0 end) as jan_sales
        ,sum(case when d_moy = 2
    then ws_ext_sales_price* ws_quantity else 0 end) as feb_sales
        ,sum(case when d_moy = 3
    then ws_ext_sales_price* ws_quantity else 0 end) as mar_sales
        ,sum(case when d_moy = 4
    then ws_ext_sales_price* ws_quantity else 0 end) as apr_sales
        ,sum(case when d_moy = 5
    then ws_ext_sales_price* ws_quantity else 0 end) as may_sales
        ,sum(case when d_moy = 6
    then ws_ext_sales_price* ws_quantity else 0 end) as jun_sales
        ,sum(case when d_moy = 7
    then ws_ext_sales_price* ws_quantity else 0 end) as jul_sales
        ,sum(case when d_moy = 8
    then ws_ext_sales_price* ws_quantity else 0 end) as aug_sales
        ,sum(case when d_moy = 9
    then ws_ext_sales_price* ws_quantity else 0 end) as sep_sales
        ,sum(case when d_moy = 10
    then ws_ext_sales_price* ws_quantity else 0 end) as oct_sales
        ,sum(case when d_moy = 11
    then ws_ext_sales_price* ws_quantity else 0 end) as nov_sales
        ,sum(case when d_moy = 12
    then ws_ext_sales_price* ws_quantity else 0 end) as dec_sales
        ,sum(case when d_moy = 1
    then ws_net_paid * ws_quantity else 0 end) as jan_net
        ,sum(case when d_moy = 2
    then ws_net_paid * ws_quantity else 0 end) as feb_net
        ,sum(case when d_moy = 3
    then ws_net_paid * ws_quantity else 0 end) as mar_net
        ,sum(case when d_moy = 4
    then ws_net_paid * ws_quantity else 0 end) as apr_net
        ,sum(case when d_moy = 5
    then ws_net_paid * ws_quantity else 0 end) as may_net
        ,sum(case when d_moy = 6
    then ws_net_paid * ws_quantity else 0 end) as jun_net
        ,sum(case when d_moy = 7
    then ws_net_paid * ws_quantity else 0 end) as jul_net
        ,sum(case when d_moy = 8
    then ws_net_paid * ws_quantity else 0 end) as aug_net
        ,sum(case when d_moy = 9
    then ws_net_paid * ws_quantity else 0 end) as sep_net
        ,sum(case when d_moy = 10
    then ws_net_paid * ws_quantity else 0 end) as oct_net
        ,sum(case when d_moy = 11
    then ws_net_paid * ws_quantity else 0 end) as nov_net
        ,sum(case when d_moy = 12
    then ws_net_paid * ws_quantity else 0 end) as dec_net
    from
    {WEB_SALES_TABLE}
        ,{WAREHOUSE_TABLE}
        ,{DATE_DIM_TABLE}
        ,{TIME_DIM_TABLE}
        ,{SHIP_MODE_TABLE}
    where
    ws_warehouse_sk =  w_warehouse_sk
    and ws_sold_date_sk = d_date_sk
    and ws_sold_time_sk = t_time_sk
    and ws_ship_mode_sk = sm_ship_mode_sk
    and d_year = 1998
    and t_time between 46866 and 46866+28800
    and sm_carrier in ('GREAT EASTERN','UPS')
    group by
    w_warehouse_name
        ,w_warehouse_sq_ft
        ,w_city
        ,w_county
        ,w_state
        ,w_country
        ,d_year
    union all
    select
    w_warehouse_name
        ,w_warehouse_sq_ft
        ,w_city
        ,w_county
        ,w_state
        ,w_country
        ,'GREAT EASTERN' || ',' || 'UPS' as ship_carriers
        ,d_year as year
        ,sum(case when d_moy = 1
    then cs_sales_price* cs_quantity else 0 end) as jan_sales
        ,sum(case when d_moy = 2
    then cs_sales_price* cs_quantity else 0 end) as feb_sales
        ,sum(case when d_moy = 3
    then cs_sales_price* cs_quantity else 0 end) as mar_sales
        ,sum(case when d_moy = 4
    then cs_sales_price* cs_quantity else 0 end) as apr_sales
        ,sum(case when d_moy = 5
    then cs_sales_price* cs_quantity else 0 end) as may_sales
        ,sum(case when d_moy = 6
    then cs_sales_price* cs_quantity else 0 end) as jun_sales
        ,sum(case when d_moy = 7
    then cs_sales_price* cs_quantity else 0 end) as jul_sales
        ,sum(case when d_moy = 8
    then cs_sales_price* cs_quantity else 0 end) as aug_sales
        ,sum(case when d_moy = 9
    then cs_sales_price* cs_quantity else 0 end) as sep_sales
        ,sum(case when d_moy = 10
    then cs_sales_price* cs_quantity else 0 end) as oct_sales
        ,sum(case when d_moy = 11
    then cs_sales_price* cs_quantity else 0 end) as nov_sales
        ,sum(case when d_moy = 12
    then cs_sales_price* cs_quantity else 0 end) as dec_sales
        ,sum(case when d_moy = 1
    then cs_net_paid * cs_quantity else 0 end) as jan_net
        ,sum(case when d_moy = 2
    then cs_net_paid * cs_quantity else 0 end) as feb_net
        ,sum(case when d_moy = 3
    then cs_net_paid * cs_quantity else 0 end) as mar_net
        ,sum(case when d_moy = 4
    then cs_net_paid * cs_quantity else 0 end) as apr_net
        ,sum(case when d_moy = 5
    then cs_net_paid * cs_quantity else 0 end) as may_net
        ,sum(case when d_moy = 6
    then cs_net_paid * cs_quantity else 0 end) as jun_net
        ,sum(case when d_moy = 7
    then cs_net_paid * cs_quantity else 0 end) as jul_net
        ,sum(case when d_moy = 8
    then cs_net_paid * cs_quantity else 0 end) as aug_net
        ,sum(case when d_moy = 9
    then cs_net_paid * cs_quantity else 0 end) as sep_net
        ,sum(case when d_moy = 10
    then cs_net_paid * cs_quantity else 0 end) as oct_net
        ,sum(case when d_moy = 11
    then cs_net_paid * cs_quantity else 0 end) as nov_net
        ,sum(case when d_moy = 12
    then cs_net_paid * cs_quantity else 0 end) as dec_net
    from
    {CATALOG_SALES_TABLE}
        ,{WAREHOUSE_TABLE}
        ,{DATE_DIM_TABLE}
        ,{TIME_DIM_TABLE}
        ,{SHIP_MODE_TABLE}
    where
    cs_warehouse_sk =  w_warehouse_sk
    and cs_sold_date_sk = d_date_sk
    and cs_sold_time_sk = t_time_sk
    and cs_ship_mode_sk = sm_ship_mode_sk
    and d_year = 1998
    and t_time between 46866 AND 46866+28800
    and sm_carrier in ('GREAT EASTERN','UPS')
    group by
    w_warehouse_name
        ,w_warehouse_sq_ft
        ,w_city
        ,w_county
        ,w_state
        ,w_country
        ,d_year
    ) x
group by
    w_warehouse_name
        ,w_warehouse_sq_ft
        ,w_city
        ,w_county
        ,w_state
        ,w_country
        ,ship_carriers
        ,year
order by w_warehouse_name
    limit 100
                        """
    ),
    (
        "query67",
        """


        select *
from (select i_category
           ,i_class
           ,i_brand
           ,i_product_name
           ,d_year
           ,d_qoy
           ,d_moy
           ,s_store_id
           ,sumsales
           ,rank() over (partition by i_category order by sumsales desc) rk
      from (select i_category
                 ,i_class
                 ,i_brand
                 ,i_product_name
                 ,d_year
                 ,d_qoy
                 ,d_moy
                 ,s_store_id
                 ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from {STORE_SALES_TABLE}
               ,{DATE_DIM_TABLE}
               ,{STORE_TABLE}
               ,{ITEM_TABLE}
            where  ss_sold_date_sk=d_date_sk
              and ss_item_sk=i_item_sk
              and ss_store_sk = s_store_sk
              and d_month_seq between 1213 and 1213+11
            group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category
       ,i_class
       ,i_brand
       ,i_product_name
       ,d_year
       ,d_qoy
       ,d_moy
       ,s_store_id
       ,sumsales
       ,rk
    limit 100
                        """
    ),
    (
        "query68",
        """


        select c_last_name
     ,c_first_name
     ,ca_city
     ,bought_city
     ,ss_ticket_number
     ,extended_price
     ,extended_tax
     ,list_price
from (select ss_ticket_number
           ,ss_customer_sk
           ,ca_city bought_city
           ,sum(ss_ext_sales_price) extended_price
           ,sum(ss_ext_list_price) list_price
           ,sum(ss_ext_tax) extended_tax
      from {STORE_SALES_TABLE}
         ,{DATE_DIM_TABLE}
         ,{STORE_TABLE}
         ,{HOUSEHOLD_DEMOGRAPHICS_TABLE}
         ,{CUSTOMER_ADDRESS_TABLE}
      where {STORE_SALES_TABLE}.ss_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
        and {STORE_SALES_TABLE}.ss_store_sk = {STORE_TABLE}.s_store_sk
        and {STORE_SALES_TABLE}.ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
        and {STORE_SALES_TABLE}.ss_addr_sk = {CUSTOMER_ADDRESS_TABLE}.ca_address_sk
        and {DATE_DIM_TABLE}.d_dom between 1 and 2
        and ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 3 or
             {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count= 3)
        and {DATE_DIM_TABLE}.d_year in (2000,2000+1,2000+2)
        and {STORE_TABLE}.s_city in ('Walnut Grove','Enterprise')
      group by ss_ticket_number
             ,ss_customer_sk
             ,ss_addr_sk,ca_city) dn
   ,{CUSTOMER_TABLE}
   ,{CUSTOMER_ADDRESS_TABLE} current_addr
where ss_customer_sk = c_customer_sk
  and {CUSTOMER_TABLE}.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> bought_city
order by c_last_name
       ,ss_ticket_number
    limit 100
                        """
    ),
    (
        "query69",
        """


        select cd_gender,
    cd_marital_status,
    cd_education_status,
    count(*) cnt1,
    cd_purchase_estimate,
    count(*) cnt2,
    cd_credit_rating,
    count(*) cnt3
from
    {CUSTOMER_TABLE} c,{CUSTOMER_ADDRESS_TABLE} ca,{CUSTOMER_DEMOGRAPHICS_TABLE}
where
        c.c_current_addr_sk = ca.ca_address_sk and
        ca_state in ('IA','GA','MN') and
        cd_demo_sk = c.c_current_cdemo_sk and
    exists (select *
            from {STORE_SALES_TABLE},{DATE_DIM_TABLE}
            where c.c_customer_sk = ss_customer_sk and
                    ss_sold_date_sk = d_date_sk and
                    d_year = 2001 and
                d_moy between 4 and 4+2) and
    (not exists (select *
                 from {WEB_SALES_TABLE},{DATE_DIM_TABLE}
                 where c.c_customer_sk = ws_bill_customer_sk and
                         ws_sold_date_sk = d_date_sk and
                         d_year = 2001 and
                     d_moy between 4 and 4+2) and
     not exists (select *
                 from {CATALOG_SALES_TABLE},{DATE_DIM_TABLE}
                 where c.c_customer_sk = cs_ship_customer_sk and
                         cs_sold_date_sk = d_date_sk and
                         d_year = 2001 and
                     d_moy between 4 and 4+2))
group by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
order by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
    limit 100
                        """
    ),
    (
        "query70",
        """


        select sum(ss_net_profit) as total_sum
     ,s_state
     ,s_county
     ,grouping(s_state)+grouping(s_county) as lochierarchy
     ,rank() over (
 	partition by grouping(s_state)+grouping(s_county),
 	case when grouping(s_county) = 0 then s_state end
 	order by sum(ss_net_profit) desc) as rank_within_parent
from
    {STORE_SALES_TABLE}
   ,{DATE_DIM_TABLE}       d1
   ,{STORE_TABLE}
where
    d1.d_month_seq between 1210 and 1210+11
  and d1.d_date_sk = ss_sold_date_sk
  and s_store_sk  = ss_store_sk
  and s_state in
      ( select s_state
        from  (select s_state as s_state,
                      rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
               from   {STORE_SALES_TABLE}, {STORE_TABLE}, {DATE_DIM_TABLE}
               where  d_month_seq between 1210 and 1210+11
                 and d_date_sk = ss_sold_date_sk
                 and s_store_sk  = ss_store_sk
               group by s_state
              ) tmp1
        where ranking <= 5
      )
group by rollup(s_state,s_county)
order by
    lochierarchy desc
       ,case when lochierarchy = 0 then s_state end
       ,rank_within_parent
    limit 100
                        """
    ),
    (
        "query71",
        """


        select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
                                                                   sum(ext_price) ext_price
from {ITEM_TABLE}, (select ws_ext_sales_price as ext_price,
                   ws_sold_date_sk as sold_date_sk,
                   ws_item_sk as sold_item_sk,
                   ws_sold_time_sk as time_sk
            from {WEB_SALES_TABLE},{DATE_DIM_TABLE}
            where d_date_sk = ws_sold_date_sk
              and d_moy=11
              and d_year=2001
            union all
            select cs_ext_sales_price as ext_price,
                   cs_sold_date_sk as sold_date_sk,
                   cs_item_sk as sold_item_sk,
                   cs_sold_time_sk as time_sk
            from {CATALOG_SALES_TABLE},{DATE_DIM_TABLE}
            where d_date_sk = cs_sold_date_sk
              and d_moy=11
              and d_year=2001
            union all
            select ss_ext_sales_price as ext_price,
                   ss_sold_date_sk as sold_date_sk,
                   ss_item_sk as sold_item_sk,
                   ss_sold_time_sk as time_sk
            from {STORE_SALES_TABLE},{DATE_DIM_TABLE}
            where d_date_sk = ss_sold_date_sk
              and d_moy=11
              and d_year=2001
) tmp,{TIME_DIM_TABLE}
where
        sold_item_sk = i_item_sk
  and i_manager_id=1
  and time_sk = t_time_sk
  and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
group by i_brand, i_brand_id,t_hour,t_minute
order by ext_price desc, i_brand_id
                        """
    ),
    (
        "query72",
        """


        select i_item_desc
     ,w_warehouse_name
     ,d1.d_week_seq
     ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
     ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
     ,count(*) total_cnt
from {CATALOG_SALES_TABLE}
         join {INVENTORY_TABLE} on (cs_item_sk = inv_item_sk)
         join {WAREHOUSE_TABLE} on (w_warehouse_sk=inv_warehouse_sk)
         join {ITEM_TABLE} on (i_item_sk = cs_item_sk)
         join {CUSTOMER_DEMOGRAPHICS_TABLE} on (cs_bill_cdemo_sk = cd_demo_sk)
         join {HOUSEHOLD_DEMOGRAPHICS_TABLE} on (cs_bill_hdemo_sk = hd_demo_sk)
         join {DATE_DIM_TABLE} d1 on (cs_sold_date_sk = d1.d_date_sk)
         join {DATE_DIM_TABLE} d2 on (inv_date_sk = d2.d_date_sk)
         join {DATE_DIM_TABLE} d3 on (cs_ship_date_sk = d3.d_date_sk)
         left outer join {PROMOTION_TABLE} on (cs_promo_sk=p_promo_sk)
         left outer join {CATALOG_RETURNS_TABLE} on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity
  and d3.d_date > dateadd(day,5,d1.d_date)
  and hd_buy_potential = '>10000'
  and d1.d_year = 2002
  and cd_marital_status = 'M'
group by i_item_desc,w_warehouse_name,d1.d_week_seq
order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
    limit 100
                        """
    ),
    (
        "query73",
        """


        select c_last_name
     ,c_first_name
     ,c_salutation
     ,c_preferred_cust_flag
     ,ss_ticket_number
     ,cnt from
              (select ss_ticket_number
                    ,ss_customer_sk
                    ,count(*) cnt
               from {STORE_SALES_TABLE},{DATE_DIM_TABLE},{STORE_TABLE},{HOUSEHOLD_DEMOGRAPHICS_TABLE}
               where {STORE_SALES_TABLE}.ss_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
                 and {STORE_SALES_TABLE}.ss_store_sk = {STORE_TABLE}.s_store_sk
                 and {STORE_SALES_TABLE}.ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
                 and {DATE_DIM_TABLE}.d_dom between 1 and 2
                 and ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_buy_potential = '>10000' or
                      {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_buy_potential = '0-500')
                 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count > 0
                 and case when {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count > 0 then
                                  {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count/ {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count else null end > 1
                 and {DATE_DIM_TABLE}.d_year in (1998,1998+1,1998+2)
                 and {STORE_TABLE}.s_county in ('Orange County','San Miguel County','Williamson County','Brazos County')
               group by ss_ticket_number,ss_customer_sk) dj,{CUSTOMER_TABLE}
where ss_customer_sk = c_customer_sk
  and cnt between 1 and 5
order by cnt desc, c_last_name asc
                        """
    ),
    (
        "query74",
        """


        with year_total as (
    select c_customer_id customer_id
         ,c_first_name customer_first_name
         ,c_last_name customer_last_name
         ,d_year as year
   ,min(ss_net_paid) year_total
   ,'s' sale_type
from {CUSTOMER_TABLE}
        ,{STORE_SALES_TABLE}
        ,{DATE_DIM_TABLE}
where c_customer_sk = ss_customer_sk
  and ss_sold_date_sk = d_date_sk
  and d_year in (2001,2001+1)
group by c_customer_id
        ,c_first_name
        ,c_last_name
        ,d_year
union all
select c_customer_id customer_id
     ,c_first_name customer_first_name
     ,c_last_name customer_last_name
     ,d_year as year
       ,min(ws_net_paid) year_total
       ,'w' sale_type
from {CUSTOMER_TABLE}
        ,{WEB_SALES_TABLE}
        ,{DATE_DIM_TABLE}
where c_customer_sk = ws_bill_customer_sk
  and ws_sold_date_sk = d_date_sk
  and d_year in (2001,2001+1)
group by c_customer_id
        ,c_first_name
        ,c_last_name
        ,d_year
    )
select
    t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
from year_total t_s_firstyear
   ,year_total t_s_secyear
   ,year_total t_w_firstyear
   ,year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.year = 2001
  and t_s_secyear.year = 2001+1
  and t_w_firstyear.year = 2001
  and t_w_secyear.year = 2001+1
  and t_s_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
    > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
order by 3,2,1
    limit 100
                        """
    ),
    (
        "query75",
        """


        WITH all_sales AS (
    SELECT d_year
         ,i_brand_id
         ,i_class_id
         ,i_category_id
         ,i_manufact_id
         ,SUM(sales_cnt) AS sales_cnt
         ,SUM(sales_amt) AS sales_amt
    FROM (SELECT d_year
               ,i_brand_id
               ,i_class_id
               ,i_category_id
               ,i_manufact_id
               ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
               ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
          FROM {CATALOG_SALES_TABLE} JOIN {ITEM_TABLE} ON i_item_sk=cs_item_sk
                             JOIN {DATE_DIM_TABLE} ON d_date_sk=cs_sold_date_sk
                             LEFT JOIN {CATALOG_RETURNS_TABLE} ON (cs_order_number=cr_order_number
              AND cs_item_sk=cr_item_sk)
          WHERE i_category='Shoes'
          UNION
          SELECT d_year
               ,i_brand_id
               ,i_class_id
               ,i_category_id
               ,i_manufact_id
               ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt
               ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
          FROM {STORE_SALES_TABLE} JOIN {ITEM_TABLE} ON i_item_sk=ss_item_sk
                           JOIN {DATE_DIM_TABLE} ON d_date_sk=ss_sold_date_sk
                           LEFT JOIN {STORE_RETURNS_TABLE} ON (ss_ticket_number=sr_ticket_number
              AND ss_item_sk=sr_item_sk)
          WHERE i_category='Shoes'
          UNION
          SELECT d_year
               ,i_brand_id
               ,i_class_id
               ,i_category_id
               ,i_manufact_id
               ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt
               ,ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
          FROM {WEB_SALES_TABLE} JOIN {ITEM_TABLE} ON i_item_sk=ws_item_sk
                         JOIN {DATE_DIM_TABLE} ON d_date_sk=ws_sold_date_sk
                         LEFT JOIN {WEB_RETURNS_TABLE} ON (ws_order_number=wr_order_number
              AND ws_item_sk=wr_item_sk)
          WHERE i_category='Shoes') sales_detail
    GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id)
SELECT  prev_yr.d_year AS prev_year
     ,curr_yr.d_year AS year
                          ,curr_yr.i_brand_id
                          ,curr_yr.i_class_id
                          ,curr_yr.i_category_id
                          ,curr_yr.i_manufact_id
                          ,prev_yr.sales_cnt AS prev_yr_cnt
                          ,curr_yr.sales_cnt AS curr_yr_cnt
                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
FROM all_sales curr_yr, all_sales prev_yr
WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
  AND curr_yr.i_class_id=prev_yr.i_class_id
  AND curr_yr.i_category_id=prev_yr.i_category_id
  AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
  AND curr_yr.d_year=2000
  AND prev_yr.d_year=2000-1
  AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
ORDER BY sales_cnt_diff,sales_amt_diff
    limit 100
                        """
    ),
    (
        "query76",
        """


        select channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
                                                                                                                                                                              SELECT '{STORE_TABLE}' as channel, 'ss_hdemo_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
                                                                                                                                                                              FROM {STORE_SALES_TABLE}, {ITEM_TABLE}, {DATE_DIM_TABLE}
                                                                                                                                                                              WHERE ss_hdemo_sk IS NULL
                                                                                                                                                                                AND ss_sold_date_sk=d_date_sk
                                                                                                                                                                                AND ss_item_sk=i_item_sk
                                                                                                                                                                              UNION ALL
                                                                                                                                                                              SELECT 'web' as channel, 'ws_ship_mode_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
                                                                                                                                                                              FROM {WEB_SALES_TABLE}, {ITEM_TABLE}, {DATE_DIM_TABLE}
                                                                                                                                                                              WHERE ws_ship_mode_sk IS NULL
                                                                                                                                                                                AND ws_sold_date_sk=d_date_sk
                                                                                                                                                                                AND ws_item_sk=i_item_sk
                                                                                                                                                                              UNION ALL
                                                                                                                                                                              SELECT 'catalog' as channel, 'cs_ship_customer_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
                                                                                                                                                                              FROM {CATALOG_SALES_TABLE}, {ITEM_TABLE}, {DATE_DIM_TABLE}
                                                                                                                                                                              WHERE cs_ship_customer_sk IS NULL
                                                                                                                                                                                AND cs_sold_date_sk=d_date_sk
                                                                                                                                                                                AND cs_item_sk=i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
    limit 100
                        """
    ),
    (
        "query77",
        """


        with ss as
                                                                     (select s_store_sk,
                                                                             sum(ss_ext_sales_price) as sales,
                                                                             sum(ss_net_profit) as profit
                                                                      from {STORE_SALES_TABLE},
                                                                           {DATE_DIM_TABLE},
                                                                           {STORE_TABLE}
                                                                      where ss_sold_date_sk = d_date_sk
                                                                        and d_date between cast('1998-08-21' as date)
                                                                          and (dateadd(day,30,to_date('1998-08-21')))
                                                                        and ss_store_sk = s_store_sk
                                                                      group by s_store_sk)
        ,
                                                                 sr as
                                                                     (select s_store_sk,
                                                                             sum(sr_return_amt) as returns,
                                                                             sum(sr_net_loss) as profit_loss
                                                                      from {STORE_RETURNS_TABLE},
                                                                           {DATE_DIM_TABLE},
                                                                           {STORE_TABLE}
                                                                      where sr_returned_date_sk = d_date_sk
                                                                        and d_date between cast('1998-08-21' as date)
                                                                          and (dateadd(day,30,to_date('1998-08-21')))
                                                                        and sr_store_sk = s_store_sk
                                                                      group by s_store_sk),
                                                                 cs as
                                                                     (select cs_call_center_sk,
                                                                             sum(cs_ext_sales_price) as sales,
                                                                             sum(cs_net_profit) as profit
                                                                      from {CATALOG_SALES_TABLE},
                                                                           {DATE_DIM_TABLE}
                                                                      where cs_sold_date_sk = d_date_sk
                                                                        and d_date between cast('1998-08-21' as date)
                                                                          and (dateadd(day,30,to_date('1998-08-21')))
                                                                      group by cs_call_center_sk
                                                                     ),
                                                                 cr as
                                                                     (select cr_call_center_sk,
                                                                             sum(cr_return_amount) as returns,
                                                                             sum(cr_net_loss) as profit_loss
                                                                      from {CATALOG_RETURNS_TABLE},
                                                                           {DATE_DIM_TABLE}
                                                                      where cr_returned_date_sk = d_date_sk
                                                                        and d_date between cast('1998-08-21' as date)
                                                                          and (dateadd(day,30,to_date('1998-08-21')))
                                                                      group by cr_call_center_sk
                                                                     ),
                                                                 ws as
                                                                     ( select wp_web_page_sk,
                                                                              sum(ws_ext_sales_price) as sales,
                                                                              sum(ws_net_profit) as profit
                                                                       from {WEB_SALES_TABLE},
                                                                            {DATE_DIM_TABLE},
                                                                            {WEB_PAGE_TABLE}
                                                                       where ws_sold_date_sk = d_date_sk
                                                                         and d_date between cast('1998-08-21' as date)
                                                                           and (dateadd(day,30,to_date('1998-08-21')))
                                                                         and ws_web_page_sk = wp_web_page_sk
                                                                       group by wp_web_page_sk),
                                                                 wr as
                                                                     (select wp_web_page_sk,
                                                                             sum(wr_return_amt) as returns,
                                                                             sum(wr_net_loss) as profit_loss
                                                                      from {WEB_RETURNS_TABLE},
                                                                           {DATE_DIM_TABLE},
                                                                           {WEB_PAGE_TABLE}
                                                                      where wr_returned_date_sk = d_date_sk
                                                                        and d_date between cast('1998-08-21' as date)
                                                                          and (dateadd(day,30,to_date('1998-08-21')))
                                                                        and wr_web_page_sk = wp_web_page_sk
                                                                      group by wp_web_page_sk)
select  channel
     , id
     , sum(sales) as sales
     , sum(returns) as returns
     , sum(profit) as profit
from
    (select '{STORE_TABLE} channel' as channel
          , ss.s_store_sk as id
          , sales
          , coalesce(returns, 0) as returns
          , (profit - coalesce(profit_loss,0)) as profit
     from   ss left join sr
                         on  ss.s_store_sk = sr.s_store_sk
     union all
     select 'catalog channel' as channel
          , cs_call_center_sk as id
          , sales
          , returns
          , (profit - profit_loss) as profit
     from  cs
        , cr
     union all
     select 'web channel' as channel
          , ws.wp_web_page_sk as id
          , sales
          , coalesce(returns, 0) returns
          , (profit - coalesce(profit_loss,0)) as profit
     from   ws left join wr
                         on  ws.wp_web_page_sk = wr.wp_web_page_sk
    ) x
group by rollup (channel, id)
order by channel
       ,id
    limit 100
                        """
    ),
    (
        "query78",
        """


        with ws as
                                                                     (select d_year AS ws_sold_year, ws_item_sk,
                                                                             ws_bill_customer_sk ws_customer_sk,
                                                                             sum(ws_quantity) ws_qty,
                                                                             sum(ws_wholesale_cost) ws_wc,
                                                                             sum(ws_sales_price) ws_sp
                                                                      from {WEB_SALES_TABLE}
                                                                               left join {WEB_RETURNS_TABLE} on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
                                                                               join {DATE_DIM_TABLE} on ws_sold_date_sk = d_date_sk
                                                                      where wr_order_number is null
                                                                      group by d_year, ws_item_sk, ws_bill_customer_sk
                                                                     ),
                                                                 cs as
                                                                     (select d_year AS cs_sold_year, cs_item_sk,
                                                                             cs_bill_customer_sk cs_customer_sk,
                                                                             sum(cs_quantity) cs_qty,
                                                                             sum(cs_wholesale_cost) cs_wc,
                                                                             sum(cs_sales_price) cs_sp
                                                                      from {CATALOG_SALES_TABLE}
                                                                               left join {CATALOG_RETURNS_TABLE} on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
                                                                               join {DATE_DIM_TABLE} on cs_sold_date_sk = d_date_sk
                                                                      where cr_order_number is null
                                                                      group by d_year, cs_item_sk, cs_bill_customer_sk
                                                                     ),
                                                                 ss as
                                                                     (select d_year AS ss_sold_year, ss_item_sk,
                                                                             ss_customer_sk,
                                                                             sum(ss_quantity) ss_qty,
                                                                             sum(ss_wholesale_cost) ss_wc,
                                                                             sum(ss_sales_price) ss_sp
                                                                      from {STORE_SALES_TABLE}
                                                                               left join {STORE_RETURNS_TABLE} on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
                                                                               join {DATE_DIM_TABLE} on ss_sold_date_sk = d_date_sk
                                                                      where sr_ticket_number is null
                                                                      group by d_year, ss_item_sk, ss_customer_sk
                                                                     )
select
    ss_customer_sk,
    round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
    ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
    coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
    coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
    coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
from ss
         left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)
         left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk)
where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=2001
order by
    ss_customer_sk,
    ss_qty desc, ss_wc desc, ss_sp desc,
    other_chan_qty,
    other_chan_wholesale_cost,
    other_chan_sales_price,
    ratio
    limit 100
                        """
    ),
    (
        "query79",
        """


        select c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
from
    (select ss_ticket_number
          ,ss_customer_sk
          ,{STORE_TABLE}.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
     from {STORE_SALES_TABLE},{DATE_DIM_TABLE},{STORE_TABLE},{HOUSEHOLD_DEMOGRAPHICS_TABLE}
     where {STORE_SALES_TABLE}.ss_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
       and {STORE_SALES_TABLE}.ss_store_sk = {STORE_TABLE}.s_store_sk
       and {STORE_SALES_TABLE}.ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 or {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count > 3)
       and {DATE_DIM_TABLE}.d_dow = 1
       and {DATE_DIM_TABLE}.d_year in (2000,2000+1,2000+2)
       and {STORE_TABLE}.s_number_employees between 200 and 295
     group by ss_ticket_number,ss_customer_sk,ss_addr_sk,{STORE_TABLE}.s_city) ms,{CUSTOMER_TABLE}
where ss_customer_sk = c_customer_sk
order by c_last_name,c_first_name,substr(s_city,1,30), profit
    limit 100
                        """
    ),
    (
        "query80",
        """


        with ssr as
                                                                    (select  s_store_id as store_id,
                                                                             sum(ss_ext_sales_price) as sales,
                                                                             sum(coalesce(sr_return_amt, 0)) as returns,
                                                                             sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
                                                                     from {STORE_SALES_TABLE} left outer join {STORE_RETURNS_TABLE} on
                                                                         (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
                                                                          {DATE_DIM_TABLE},
                                                                          {STORE_TABLE},
                                                                          {ITEM_TABLE},
                                                                          {PROMOTION_TABLE}
                                                                     where ss_sold_date_sk = d_date_sk
                                                                       and d_date between cast('2002-08-04' as date)
                                                                         and dateadd(day,30,to_date('2002-08-04'))
                                                                       and ss_store_sk = s_store_sk
                                                                       and ss_item_sk = i_item_sk
                                                                       and i_current_price > 50
                                                                       and ss_promo_sk = p_promo_sk
                                                                       and p_channel_tv = 'N'
                                                                     group by s_store_id)
        ,
                                                                csr as
                                                                    (select  cp_catalog_page_id as catalog_page_id,
                                                                             sum(cs_ext_sales_price) as sales,
                                                                             sum(coalesce(cr_return_amount, 0)) as returns,
                                                                             sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
                                                                     from {CATALOG_SALES_TABLE} left outer join {CATALOG_RETURNS_TABLE} on
                                                                         (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
                                                                          {DATE_DIM_TABLE},
                                                                          {CATALOG_PAGE_TABLE},
                                                                          {ITEM_TABLE},
                                                                          {PROMOTION_TABLE}
                                                                     where cs_sold_date_sk = d_date_sk
                                                                       and d_date between cast('2002-08-04' as date)
                                                                         and dateadd(day,30,to_date('2002-08-04'))
                                                                       and cs_catalog_page_sk = cp_catalog_page_sk
                                                                       and cs_item_sk = i_item_sk
                                                                       and i_current_price > 50
                                                                       and cs_promo_sk = p_promo_sk
                                                                       and p_channel_tv = 'N'
                                                                     group by cp_catalog_page_id)
        ,
                                                                wsr as
                                                                    (select  web_site_id,
                                                                             sum(ws_ext_sales_price) as sales,
                                                                             sum(coalesce(wr_return_amt, 0)) as returns,
                                                                             sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
                                                                     from {WEB_SALES_TABLE} left outer join {WEB_RETURNS_TABLE} on
                                                                         (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
                                                                          {DATE_DIM_TABLE},
                                                                          {WEB_SITE_TABLE},
                                                                          {ITEM_TABLE},
                                                                          {PROMOTION_TABLE}
                                                                     where ws_sold_date_sk = d_date_sk
                                                                       and d_date between cast('2002-08-04' as date)
                                                                         and dateadd(day,30,to_date('2002-08-04'))
                                                                       and ws_web_site_sk = web_site_sk
                                                                       and ws_item_sk = i_item_sk
                                                                       and i_current_price > 50
                                                                       and ws_promo_sk = p_promo_sk
                                                                       and p_channel_tv = 'N'
                                                                     group by web_site_id)
select  channel
     , id
     , sum(sales) as sales
     , sum(returns) as returns
     , sum(profit) as profit
from
    (select '{STORE_TABLE} channel' as channel
          , '{STORE_TABLE}' || store_id as id
          , sales
          , returns
          , profit
     from   ssr
     union all
     select 'catalog channel' as channel
          , '{CATALOG_PAGE_TABLE}' || catalog_page_id as id
          , sales
          , returns
          , profit
     from  csr
     union all
     select 'web channel' as channel
          , '{WEB_SITE_TABLE}' || web_site_id as id
          , sales
          , returns
          , profit
     from   wsr
    ) x
group by rollup (channel, id)
order by channel
       ,id
    limit 100
                        """
    ),
    (
        "query81",
        """


        with customer_total_return as
                                                                     (select cr_returning_customer_sk as ctr_customer_sk
                                                                              ,ca_state as ctr_state,
                                                                             sum(cr_return_amt_inc_tax) as ctr_total_return
                                                                      from {CATALOG_RETURNS_TABLE}
                                                                         ,{DATE_DIM_TABLE}
                                                                         ,{CUSTOMER_ADDRESS_TABLE}
                                                                      where cr_returned_date_sk = d_date_sk
                                                                        and d_year =2000
                                                                        and cr_returning_addr_sk = ca_address_sk
                                                                      group by cr_returning_customer_sk
                                                                             ,ca_state )
select  c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
     ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
     ,ca_location_type,ctr_total_return
from customer_total_return ctr1
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{CUSTOMER_TABLE}
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
                               from customer_total_return ctr2
                               where ctr1.ctr_state = ctr2.ctr_state)
  and ca_address_sk = c_current_addr_sk
  and ca_state = 'VA'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
       ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
       ,ca_location_type,ctr_total_return
    limit 100
                        """
    ),
    (
        "query82",
        """


        select i_item_id
     ,i_item_desc
     ,i_current_price
from {ITEM_TABLE}, {INVENTORY_TABLE}, {DATE_DIM_TABLE}, {STORE_SALES_TABLE}
where i_current_price between 72 and 72+30
  and inv_item_sk = i_item_sk
  and d_date_sk=inv_date_sk
  and d_date between cast('1998-01-23' as date) and dateadd(day,60,to_date('1998-01-23'))
  and i_manufact_id in (412,343,781,156)
  and inv_quantity_on_hand between 100 and 500
  and ss_item_sk = i_item_sk
group by i_item_id,i_item_desc,i_current_price
order by i_item_id
    limit 100
                        """
    ),
    (
        "query83",
        """


        with sr_items as
                                                                     (select i_item_id item_id,
                                                                             sum(sr_return_quantity) sr_item_qty
                                                                      from {STORE_RETURNS_TABLE},
                                                                           {ITEM_TABLE},
                                                                           {DATE_DIM_TABLE}
                                                                      where sr_item_sk = i_item_sk
                                                                        and   d_date    in
                                                                              (select d_date
                                                                               from {DATE_DIM_TABLE}
                                                                               where d_week_seq in
                                                                                     (select d_week_seq
                                                                                      from {DATE_DIM_TABLE}
                                                                                      where d_date in ('1998-02-20','1998-09-28','1998-11-14')))
                                                                        and   sr_returned_date_sk   = d_date_sk
                                                                      group by i_item_id),
                                                                 cr_items as
                                                                     (select i_item_id item_id,
                                                                             sum(cr_return_quantity) cr_item_qty
                                                                      from {CATALOG_RETURNS_TABLE},
                                                                           {ITEM_TABLE},
                                                                           {DATE_DIM_TABLE}
                                                                      where cr_item_sk = i_item_sk
                                                                        and   d_date    in
                                                                              (select d_date
                                                                               from {DATE_DIM_TABLE}
                                                                               where d_week_seq in
                                                                                     (select d_week_seq
                                                                                      from {DATE_DIM_TABLE}
                                                                                      where d_date in ('1998-02-20','1998-09-28','1998-11-14')))
                                                                        and   cr_returned_date_sk   = d_date_sk
                                                                      group by i_item_id),
                                                                 wr_items as
                                                                     (select i_item_id item_id,
                                                                             sum(wr_return_quantity) wr_item_qty
                                                                      from {WEB_RETURNS_TABLE},
                                                                           {ITEM_TABLE},
                                                                           {DATE_DIM_TABLE}
                                                                      where wr_item_sk = i_item_sk
                                                                        and   d_date    in
                                                                              (select d_date
                                                                               from {DATE_DIM_TABLE}
                                                                               where d_week_seq in
                                                                                     (select d_week_seq
                                                                                      from {DATE_DIM_TABLE}
                                                                                      where d_date in ('1998-02-20','1998-09-28','1998-11-14')))
                                                                        and   wr_returned_date_sk   = d_date_sk
                                                                      group by i_item_id)
select  sr_items.item_id
     ,sr_item_qty
     ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
     ,cr_item_qty
     ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
     ,wr_item_qty
     ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
     ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
from sr_items
   ,cr_items
   ,wr_items
where sr_items.item_id=cr_items.item_id
  and sr_items.item_id=wr_items.item_id
order by sr_items.item_id
       ,sr_item_qty
    limit 100
                        """
    ),
    (
        "query84",
        """


        select c_customer_id as customer_id
     , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
from {CUSTOMER_TABLE}
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{CUSTOMER_DEMOGRAPHICS_TABLE}
   ,{HOUSEHOLD_DEMOGRAPHICS_TABLE}
   ,{INCOME_BAND_TABLE}
   ,{STORE_RETURNS_TABLE}
where ca_city	        =  'Shady Grove'
  and c_current_addr_sk = ca_address_sk
  and ib_lower_bound   >=  29438
  and ib_upper_bound   <=  29438 + 50000
  and ib_income_band_sk = hd_income_band_sk
  and cd_demo_sk = c_current_cdemo_sk
  and hd_demo_sk = c_current_hdemo_sk
  and sr_cdemo_sk = cd_demo_sk
order by c_customer_id
    limit 100
                        """
    ),
    (
        "query85",
        """


        select substr(r_reason_desc,1,20)
     ,avg(ws_quantity)
     ,avg(wr_refunded_cash)
     ,avg(wr_fee)
from {WEB_SALES_TABLE}, {WEB_RETURNS_TABLE}, {WEB_PAGE_TABLE}, {CUSTOMER_DEMOGRAPHICS_TABLE} cd1,
     {CUSTOMER_DEMOGRAPHICS_TABLE} cd2, {CUSTOMER_ADDRESS_TABLE}, {DATE_DIM_TABLE}, {REASON_TABLE}
where ws_web_page_sk = wp_web_page_sk
  and ws_item_sk = wr_item_sk
  and ws_order_number = wr_order_number
  and ws_sold_date_sk = d_date_sk and d_year = 1998
  and cd1.cd_demo_sk = wr_refunded_cdemo_sk
  and cd2.cd_demo_sk = wr_returning_cdemo_sk
  and ca_address_sk = wr_refunded_addr_sk
  and r_reason_sk = wr_reason_sk
  and
    (
            (
                        cd1.cd_marital_status = 'D'
                    and
                        cd1.cd_marital_status = cd2.cd_marital_status
                    and
                        cd1.cd_education_status = 'Primary'
                    and
                        cd1.cd_education_status = cd2.cd_education_status
                    and
                        ws_sales_price between 100.00 and 150.00
                )
            or
            (
                        cd1.cd_marital_status = 'U'
                    and
                        cd1.cd_marital_status = cd2.cd_marital_status
                    and
                        cd1.cd_education_status = '4 yr Degree'
                    and
                        cd1.cd_education_status = cd2.cd_education_status
                    and
                        ws_sales_price between 50.00 and 100.00
                )
            or
            (
                        cd1.cd_marital_status = 'W'
                    and
                        cd1.cd_marital_status = cd2.cd_marital_status
                    and
                        cd1.cd_education_status = 'Advanced Degree'
                    and
                        cd1.cd_education_status = cd2.cd_education_status
                    and
                        ws_sales_price between 150.00 and 200.00
                )
        )
  and
    (
            (
                        ca_country = 'United States'
                    and
                        ca_state in ('LA', 'CO', 'TX')
                    and ws_net_profit between 100 and 200
                )
            or
            (
                        ca_country = 'United States'
                    and
                        ca_state in ('OH', 'VA', 'MO')
                    and ws_net_profit between 150 and 300
                )
            or
            (
                        ca_country = 'United States'
                    and
                        ca_state in ('FL', 'OK', 'MS')
                    and ws_net_profit between 50 and 250
                )
        )
group by r_reason_desc
order by substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
    limit 100
                        """
    ),
    (
        "query86",
        """


        select sum(ws_net_paid) as total_sum
     ,i_category
     ,i_class
     ,grouping(i_category)+grouping(i_class) as lochierarchy
     ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end
 	order by sum(ws_net_paid) desc) as rank_within_parent
from
    {WEB_SALES_TABLE}
   ,{DATE_DIM_TABLE}       d1
   ,{ITEM_TABLE}
where
    d1.d_month_seq between 1205 and 1205+11
  and d1.d_date_sk = ws_sold_date_sk
  and i_item_sk  = ws_item_sk
group by rollup(i_category,i_class)
order by
    lochierarchy desc,
    case when lochierarchy = 0 then i_category end,
    rank_within_parent
    limit 100
                        """
    ),
    (
        "query87",
        """


        select count(*)
from ((select distinct c_last_name, c_first_name, d_date
       from {STORE_SALES_TABLE}, {DATE_DIM_TABLE}, {CUSTOMER_TABLE}
       where {STORE_SALES_TABLE}.ss_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
         and {STORE_SALES_TABLE}.ss_customer_sk = {CUSTOMER_TABLE}.c_customer_sk
         and d_month_seq between 1208 and 1208+11)
      except
      (select distinct c_last_name, c_first_name, d_date
       from {CATALOG_SALES_TABLE}, {DATE_DIM_TABLE}, {CUSTOMER_TABLE}
       where {CATALOG_SALES_TABLE}.cs_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
         and {CATALOG_SALES_TABLE}.cs_bill_customer_sk = {CUSTOMER_TABLE}.c_customer_sk
         and d_month_seq between 1208 and 1208+11)
      except
      (select distinct c_last_name, c_first_name, d_date
       from {WEB_SALES_TABLE}, {DATE_DIM_TABLE}, {CUSTOMER_TABLE}
       where {WEB_SALES_TABLE}.ws_sold_date_sk = {DATE_DIM_TABLE}.d_date_sk
         and {WEB_SALES_TABLE}.ws_bill_customer_sk = {CUSTOMER_TABLE}.c_customer_sk
         and d_month_seq between 1208 and 1208+11)
     ) cool_cust
                        """
    ),
    (
        "query88",
        """


        select *
from
    (select count(*) h8_30_to_9
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 8
       and {TIME_DIM_TABLE}.t_minute >= 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s1,
    (select count(*) h9_to_9_30
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 9
       and {TIME_DIM_TABLE}.t_minute < 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s2,
    (select count(*) h9_30_to_10
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 9
       and {TIME_DIM_TABLE}.t_minute >= 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s3,
    (select count(*) h10_to_10_30
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 10
       and {TIME_DIM_TABLE}.t_minute < 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s4,
    (select count(*) h10_30_to_11
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 10
       and {TIME_DIM_TABLE}.t_minute >= 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s5,
    (select count(*) h11_to_11_30
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 11
       and {TIME_DIM_TABLE}.t_minute < 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s6,
    (select count(*) h11_30_to_12
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 11
       and {TIME_DIM_TABLE}.t_minute >= 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s7,
    (select count(*) h12_to_12_30
     from {STORE_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {STORE_TABLE}
     where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
       and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
       and ss_store_sk = s_store_sk
       and {TIME_DIM_TABLE}.t_hour = 12
       and {TIME_DIM_TABLE}.t_minute < 30
       and (({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 0 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=0+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=1+2) or
            ({HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = -1 and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_vehicle_count<=-1+2))
       and {STORE_TABLE}.s_store_name = 'ese') s8
                        """
    ),
    (
        "query89",
        """


        select *
from(
        select i_category, i_class, i_brand,
               s_store_name, s_company_name,
               d_moy,
               sum(ss_sales_price) sum_sales,
               avg(sum(ss_sales_price)) over
            (partition by i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
        from {ITEM_TABLE}, {STORE_SALES_TABLE}, {DATE_DIM_TABLE}, {STORE_TABLE}
        where ss_item_sk = i_item_sk and
                ss_sold_date_sk = d_date_sk and
                ss_store_sk = s_store_sk and
                d_year in (2000) and
            ((i_category in ('Electronics','Women','Home') and
              i_class in ('cameras','maternity','kids')
                 )
                or (i_category in ('Sports','Jewelry','Shoes') and
                    i_class in ('guns','semi-precious','athletic')
                 ))
        group by i_category, i_class, i_brand,
                 s_store_name, s_company_name, d_moy) tmp1
where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
    limit 100
                        """
    ),
    (
        "query90",
        """


        select cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
from ( select count(*) amc
       from {WEB_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {WEB_PAGE_TABLE}
       where ws_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
         and ws_ship_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
         and ws_web_page_sk = {WEB_PAGE_TABLE}.wp_web_page_sk
         and {TIME_DIM_TABLE}.t_hour between 8 and 8+1
         and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 9
         and {WEB_PAGE_TABLE}.wp_char_count between 5000 and 5200) at,
      ( select count(*) pmc
       from {WEB_SALES_TABLE}, {HOUSEHOLD_DEMOGRAPHICS_TABLE} , {TIME_DIM_TABLE}, {WEB_PAGE_TABLE}
       where ws_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
         and ws_ship_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
         and ws_web_page_sk = {WEB_PAGE_TABLE}.wp_web_page_sk
         and {TIME_DIM_TABLE}.t_hour between 18 and 18+1
         and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 9
         and {WEB_PAGE_TABLE}.wp_char_count between 5000 and 5200) pt
order by am_pm_ratio
    limit 100
                        """
    ),
    (
        "query91",
        """


        select cc_call_center_id Call_Center,
    cc_name Call_Center_Name,
    cc_manager Manager,
    sum(cr_net_loss) Returns_Loss
from
    {CALL_CENTER_TABLE},
    {CATALOG_RETURNS_TABLE},
    {DATE_DIM_TABLE},
    {CUSTOMER_TABLE},
    {CUSTOMER_ADDRESS_TABLE},
    {CUSTOMER_DEMOGRAPHICS_TABLE},
    {HOUSEHOLD_DEMOGRAPHICS_TABLE}
where
        cr_call_center_sk       = cc_call_center_sk
  and     cr_returned_date_sk     = d_date_sk
  and     cr_returning_customer_sk= c_customer_sk
  and     cd_demo_sk              = c_current_cdemo_sk
  and     hd_demo_sk              = c_current_hdemo_sk
  and     ca_address_sk           = c_current_addr_sk
  and     d_year                  = 2002
  and     d_moy                   = 11
  and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
    or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
  and     hd_buy_potential like 'Unknown%'
  and     ca_gmt_offset           = -6
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by sum(cr_net_loss) desc
                        """
    ),
    (
        "query92",
        """


        select sum(ws_ext_discount_amt)  as "Excess Discount Amount"
from
    {WEB_SALES_TABLE}
   ,{ITEM_TABLE}
   ,{DATE_DIM_TABLE}
where
        i_manufact_id = 939
  and i_item_sk = ws_item_sk
  and d_date between '2002-02-16' and
    dateadd(day,90,to_date('2002-02-16'))
  and d_date_sk = ws_sold_date_sk
  and ws_ext_discount_amt
    > (
          SELECT
                  1.3 * avg(ws_ext_discount_amt)
          FROM
              {WEB_SALES_TABLE}
             ,{DATE_DIM_TABLE}
          WHERE
                  ws_item_sk = i_item_sk
            and d_date between '2002-02-16' and
              dateadd(day,90,to_date('2002-02-16'))
            and d_date_sk = ws_sold_date_sk
      )
order by sum(ws_ext_discount_amt)
    limit 100
                        """
    ),
    (
        "query93",
        """


        select ss_customer_sk
     ,sum(act_sales) sumsales
from (select ss_item_sk
           ,ss_ticket_number
           ,ss_customer_sk
           ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                 else (ss_quantity*ss_sales_price) end act_sales
      from {STORE_SALES_TABLE} left outer join {STORE_RETURNS_TABLE} on (sr_item_sk = ss_item_sk
          and sr_ticket_number = ss_ticket_number)
         ,{REASON_TABLE}
      where sr_reason_sk = r_reason_sk
        and r_reason_desc = '{REASON_TABLE} 75') t
group by ss_customer_sk
order by sumsales, ss_customer_sk
    limit 100
                        """
    ),
    (
        "query94",
        """


        select count(distinct ws_order_number) as "order count"
     ,sum(ws_ext_ship_cost) as "total shipping cost"
     ,sum(ws_net_profit) as "total net profit"
from
    {WEB_SALES_TABLE} ws1
   ,{DATE_DIM_TABLE}
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{WEB_SITE_TABLE}
where
    d_date between '2002-2-01' and
        dateadd(day,60,to_date('2002-2-01'))
  and ws1.ws_ship_date_sk = d_date_sk
  and ws1.ws_ship_addr_sk = ca_address_sk
  and ca_state = 'CA'
  and ws1.ws_web_site_sk = web_site_sk
  and web_company_name = 'pri'
  and exists (select *
              from {WEB_SALES_TABLE} ws2
              where ws1.ws_order_number = ws2.ws_order_number
                and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
  and not exists(select *
                 from {WEB_RETURNS_TABLE} wr1
                 where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
    limit 100
                        """
    ),
    (
        "query95",
        """


        with ws_wh as
                                                                     (select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
                                                                      from {WEB_SALES_TABLE} ws1,{WEB_SALES_TABLE} ws2
                                                                      where ws1.ws_order_number = ws2.ws_order_number
                                                                        and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
select
    count(distinct ws_order_number) as "order count"
     ,sum(ws_ext_ship_cost) as "total shipping cost"
     ,sum(ws_net_profit) as "total net profit"
from
    {WEB_SALES_TABLE} ws1
   ,{DATE_DIM_TABLE}
   ,{CUSTOMER_ADDRESS_TABLE}
   ,{WEB_SITE_TABLE}
where
    d_date between '2002-2-01' and
        dateadd(day,60,to_date('2002-2-01'))
  and ws1.ws_ship_date_sk = d_date_sk
  and ws1.ws_ship_addr_sk = ca_address_sk
  and ca_state = 'TN'
  and ws1.ws_web_site_sk = web_site_sk
  and web_company_name = 'pri'
  and ws1.ws_order_number in (select ws_order_number
                              from ws_wh)
  and ws1.ws_order_number in (select wr_order_number
                              from {WEB_RETURNS_TABLE},ws_wh
                              where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
    limit 100
                        """
    ),
    (
        "query96",
        """


        select count(*)
from {STORE_SALES_TABLE}
   ,{HOUSEHOLD_DEMOGRAPHICS_TABLE}
   ,{TIME_DIM_TABLE}, {STORE_TABLE}
where ss_sold_time_sk = {TIME_DIM_TABLE}.t_time_sk
  and ss_hdemo_sk = {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_demo_sk
  and ss_store_sk = s_store_sk
  and {TIME_DIM_TABLE}.t_hour = 8
  and {TIME_DIM_TABLE}.t_minute >= 30
  and {HOUSEHOLD_DEMOGRAPHICS_TABLE}.hd_dep_count = 5
  and {STORE_TABLE}.s_store_name = 'ese'
order by count(*)
    limit 100
                        """
    ),
    (
        "query97",
        """


        with ssci as (
    select ss_customer_sk customer_sk
         ,ss_item_sk item_sk
    from {STORE_SALES_TABLE},{DATE_DIM_TABLE}
    where ss_sold_date_sk = d_date_sk
      and d_month_seq between 1214 and 1214 + 11
    group by ss_customer_sk
           ,ss_item_sk),
                                                                 csci as(
                                                                     select cs_bill_customer_sk customer_sk
                                                                          ,cs_item_sk item_sk
                                                                     from {CATALOG_SALES_TABLE},{DATE_DIM_TABLE}
                                                                     where cs_sold_date_sk = d_date_sk
                                                                       and d_month_seq between 1214 and 1214 + 11
                                                                     group by cs_bill_customer_sk
                                                                            ,cs_item_sk)
select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
     ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
     ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
    and ssci.item_sk = csci.item_sk)
    limit 100
                        """
    ),
    (
        "query98",
        """


        select i_item_id
     ,i_item_desc
     ,i_category
     ,i_class
     ,i_current_price
     ,sum(ss_ext_sales_price) as itemrevenue
     ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
    (partition by i_class) as revenueratio
from
    {STORE_SALES_TABLE}
   ,{ITEM_TABLE}
   ,{DATE_DIM_TABLE}
where
        ss_item_sk = i_item_sk
  and i_category in ('Women', 'Electronics', 'Shoes')
  and ss_sold_date_sk = d_date_sk
  and d_date between cast('2002-05-27' as date)
    and dateadd(day,30,to_date('2002-05-27'))
group by
    i_item_id
       ,i_item_desc
       ,i_category
       ,i_class
       ,i_current_price
order by
    i_category
       ,i_class
       ,i_item_id
       ,i_item_desc
       ,revenueratio
                        """
    ),
    (
        "query99",
        """


        select substr(w_warehouse_name,1,20)
     ,sm_type
     ,cc_name
     ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
     ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and
                    (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
     ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and
                    (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
     ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                    (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
     ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
from
    {CATALOG_SALES_TABLE}
   ,{WAREHOUSE_TABLE}
   ,{SHIP_MODE_TABLE}
   ,{CALL_CENTER_TABLE}
   ,{DATE_DIM_TABLE}
where
    d_month_seq between 1200 and 1200 + 11
  and cs_ship_date_sk   = d_date_sk
  and cs_warehouse_sk   = w_warehouse_sk
  and cs_ship_mode_sk   = sm_ship_mode_sk
  and cs_call_center_sk = cc_call_center_sk
group by
    substr(w_warehouse_name,1,20)
       ,sm_type
       ,cc_name
order by substr(w_warehouse_name,1,20)
       ,sm_type
       ,cc_name
    limit 100
                        """
    )
]



def parametrize_tpcds_queries(fully_qualified_names_for_embucket):
    """
    Replace table name placeholders in TPC-DS queries with actual table names.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use just the default table names.

    Returns:
        list: A list of (query_name, parametrized_query) tuples.
    """
    return parametrize_tpcds_statements(_TPCDS_QUERIES_RAW, fully_qualified_names_for_embucket)
