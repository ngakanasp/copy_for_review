--parameter
set df_table = 'temp_ngk_producrec_df_atl_202112';
set bba_table = 'bba_broadband_daily_202112';
set laccima_date_start = '2021-12-01';
set laccima_date_end = '2021-12-31';

--create table
create table nar.${df_table} as
select sp.yearmonth,
       sp.site_id,
       sp.content_id,
       --sp.package_name,
       sp.category,
       sp.price,
       sp.site_prod_trx,
       sp.site_prod_reorder_trx,
       s.num_package_purchased,
       s.site_trx,
       s.site_reorder_trx,
       p.num_msisdn_purchaser,
       p.prod_trx,
       p.prod_reorder_trx,
       (case when sp.site_prod_reorder_trx>0 then 1 else 0 end) reordered
from
(select df.yearmonth,
       dm.site_id,
       df.content_id,
       --df.package_name,
       df.category,
       df.price,
       sum(df.trx) site_prod_trx,
       sum(case when df.trx = 0 then 0 else df.trx-1 end) site_prod_reorder_trx
from (
    select df1.yearmonth,
        df1.lacci,
        df1.msisdn,
        df1.los_segment,
        df1.content_id,
        df1.category,
        df1.price,
        sum(df1.trx) trx
from (
    select concat(year(bba.trx_date),month(bba.trx_date)) yearmonth,
       cb.lacci,
       bba.msisdn,
       bba.los_segment,
       bba.trx_date,
       bba.content_id,
       --bba.category package_name,
       concat(split(bba.package_group,'\\. ')[1],' _ ',split(bba.package_subgroup,'\\. ')[1]) category,
       bba.harga price,
       bba.trx
from mkt_hq_v.${bba_table} bba
left join (
    select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_pre_dd
    where event_date between ${laccima_date_start} and ${laccima_date_end}
    union all
    select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_post_dd
    where event_date between ${laccima_date_start} and ${laccima_date_end}
    ) cb on bba.msisdn=cb.msisdn and bba.trx_date=cb.event_date
where bba.los_segment not in ('01. 0-1mo','02. 1-3mo') and bba.package_group not in ('02. Acquisition')) df1
group by df1.yearmonth,
        df1.lacci,
        df1.msisdn,
        df1.los_segment,
        df1.content_id,
        df1.category,
        df1.price) df
left join (
    select concat(lac,ci) lacci, site_id
    from dim.laccima_dim
    where lacci is not null and lacci not in ('null', 'UNKNOWN')
      and site_id is not null and site_id not in ('null', 'UNKNOWN')
      and event_date between ${laccima_date_start} and ${laccima_date_end}
    group by concat(lac,ci), site_id) dm
on df.lacci = dm.lacci
group by df.yearmonth,
         dm.site_id,
         df.content_id,
         --df.package_name,
         df.category,
         df.price) sp
left join
(select df.yearmonth,
       dm.site_id,
       count(distinct df.content_id) num_package_purchased,
       sum(df.trx) site_trx,
       sum(case when df.trx = 0 then 0 else df.trx-1 end) site_reorder_trx
from (
    select df1.yearmonth,
        df1.lacci,
        df1.msisdn,
        df1.los_segment,
        df1.content_id,
        df1.category,
        df1.price,
        sum(df1.trx) trx
from (
    select concat(year(bba.trx_date),month(bba.trx_date)) yearmonth,
       cb.lacci,
       bba.msisdn,
       bba.los_segment,
       bba.trx_date,
       bba.content_id,
       --bba.category package_name,
       concat(split(bba.package_group,'\\. ')[1],' _ ',split(bba.package_subgroup,'\\. ')[1]) category,
       bba.harga price,
       bba.trx
from mkt_hq_v.${bba_table} bba
left join (
    select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_pre_dd
    where event_date between ${laccima_date_start} and ${laccima_date_end}
    union all
    select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_post_dd
    where event_date between ${laccima_date_start} and ${laccima_date_end}
    ) cb on bba.msisdn=cb.msisdn and bba.trx_date=cb.event_date
where bba.los_segment not in ('01. 0-1mo','02. 1-3mo') and bba.package_group not in ('02. Acquisition')) df1
group by df1.yearmonth,
        df1.lacci,
        df1.msisdn,
        df1.los_segment,
        df1.content_id,
        df1.category,
        df1.price) df
left join (
    select concat(lac,ci) lacci, site_id
    from dim.laccima_dim
    where lacci is not null and lacci not in ('null', 'UNKNOWN')
      and site_id is not null and site_id not in ('null', 'UNKNOWN')
      and event_date between ${laccima_date_start} and ${laccima_date_end}
    group by concat(lac,ci), site_id) dm
on df.lacci = dm.lacci
group by df.yearmonth,
         dm.site_id) s on sp.yearmonth=s.yearmonth and sp.site_id=s.site_id
left join
(select df.yearmonth,
       df.content_id,
       df.category,
       df.price,
       count(distinct df.msisdn) num_msisdn_purchaser,
       sum(trx) prod_trx,
       sum(case when df.trx = 0 then 0 else df.trx-1 end) prod_reorder_trx
from (
    select df1.yearmonth,
        df1.lacci,
        df1.msisdn,
        df1.los_segment,
        df1.content_id,
        df1.category,
        df1.price,
        sum(df1.trx) trx
from (
    select concat(year(bba.trx_date),month(bba.trx_date)) yearmonth,
       cb.lacci,
       bba.msisdn,
       bba.los_segment,
       bba.trx_date,
       bba.content_id,
       --bba.category package_name,
       concat(split(bba.package_group,'\\. ')[1],' _ ',split(bba.package_subgroup,'\\. ')[1]) category,
       bba.harga price,
       bba.trx
from mkt_hq_v.${bba_table} bba
left join (
    select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_pre_dd
    where event_date between ${laccima_date_start} and ${laccima_date_end}
    union all
    select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_post_dd
    where event_date between ${laccima_date_start} and ${laccima_date_end}
    ) cb on bba.msisdn=cb.msisdn and bba.trx_date=cb.event_date
where bba.los_segment not in ('01. 0-1mo','02. 1-3mo') and bba.package_group not in ('02. Acquisition')) df1
group by df1.yearmonth,
        df1.lacci,
        df1.msisdn,
        df1.los_segment,
        df1.content_id,
        df1.category,
        df1.price) df
group by df.yearmonth,
         df.content_id,
         df.category,
         df.price) p on sp.yearmonth=p.yearmonth and sp.content_id=p.content_id and sp.category=p.category and sp.price=p.price
where sp.site_id is not null and sp.price > 1000;


--check table
show tables in nar '*temp_ng*';
drop table if exists nar.temp_ngk_producrec_df_atl_202109-;
select * from nar.temp_ngk_producrec_df_atl_202201 limit 50;

--export dataset
select a.* from (select *, ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_prod_trx desc) as rn from nar.temp_ngk_producrec_df_atl_202109) a;
