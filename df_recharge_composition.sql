--recharge composition
create table nar.temp_ngk_optimumoutlet_rech_${df_table} as
select sop.period,
       sop.trx_date,
       sop.report_group,
       sop.report_type,
       sop.channel_group,
       sop.channel_category,
       sop.channel_name,
       sop.denom_interval,
       sop.lacci,
       site.site_id,
       sum(sop.total_trx) total_trx,
       sum(sop.total_amount) total_amount
from (select SUBSTRING(rech.trx_date,1,7) as period,
             rech.trx_date,
             case
                 when (rech.ngrs_flag in('MKIOS','NM','M','','NA','N/A')
                     or rech.ngrs_flag is null) then 'TRADITIONAL'
                 when (rech.ngrs_flag in('URP','NU','U')
                     and reff.report_type is not null and upper(reff.report_type) in('DEALER','FISIK')) then 'TRADITIONAL'
                 else 'MODERN' end as report_group,
             case
                 when reff.report_type is not null then reff.report_type
                 when rech.ngrs_flag in ('URP','NU','U')
                     and reff.report_type is null then 'OTHERS'
                 when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name like '%ADOL%' then 'DEALER'
                 when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name not like '%ADOL%' then 'OUTLET'
                 else 'OTHERS ' end as report_type,
             case
                 when reff.channel_group is not null then reff.channel_group
                 when rech.ngrs_flag in ('URP','NU','U')
                     and reff.channel_group is null then 'OTHERS'
                 when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name like '%ADOL%' then 'DEALER'
                 when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name not like '%ADOL%' then 'OUTLET'
                 else 'OTHERS ' end as channel_group,
             case
                 when reff.channel_category is not null then reff.channel_category
                 when rech.ngrs_flag in('URP','NU','U')
                     and reff.channel_category is null then 'OTHERS'
                 when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name like '%ADOL%' then 'AD ONLINE'
                 when (rech.ngrs_flag in('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name not like '%ADOL%' then 'AUTHORIZED DEALER'
                 else 'OTHERS ' end as channel_category,
             case
                 when reff.channel_name is not null then reff.channel_name
                 when rech.ngrs_flag in ('URP','NU','U')
                     and reff.channel_name is null then 'OTHERS'
                 when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name like '%FINNETADOL%' then 'FINNET'
                 when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name like '%KISELADOL%' then 'KISEL'
                 when (rech.ngrs_flag in('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                     and rech.upstream_name not like '%ADOL%' then 'OUTLET'
                 else 'OTHERS ' end as channel_name,
             case
                 when rech.denom<5000 then '01. <5k'
                 when rech.denom=5000 then '02. 5k'
                 when rech.denom>5000 and rech.denom<10000 then '03. 6-9k'
                 when rech.denom=10000 then '04. 10k'
                 when rech.denom>10000 and rech.denom<20000 then '05. 11-19k'
                 when rech.denom=20000 then '06. 20k'
                 when rech.denom>20000 and rech.denom<25000 then '07. 21-24k'
                 when rech.denom=25000 then '08. 25k'
                 when rech.denom>25000 and rech.denom<50000 then '09. 26-49k'
                 when rech.denom=50000 then '10. 50k'
                 when rech.denom>50000 and rech.denom<100000 then '11. 51-99k'
                 when rech.denom=100000 then '12. 100k'
                 when rech.denom>100000 then '13. >100k'
                 else rech.denom end as denom_interval,
             rech.bnum_msisdn,
             cb.lacci,
             sum(rech.trx_rech) as total_trx,
             sum(rech.rech) as total_amount
      from smy.rech_merge_hh rech
               left outer join (
          select id,
                 channel_group,
                 channel_category,
                 channel_name,
                 merchant,
                 report_type,
                 report_group,
                 source_type,
                 source_type_c
          from (
                   select id,
                          channel_group,
                          channel_category,
                          channel_name,
                          merchant,
                          report_type,
                          report_group,
                          source_type,
                          (case
                               when source_type = 'URP' then 'U'
                               when source_type = 'NGRS' then 'NU'
                               else source_type
                              end) source_type_c,
                          ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_date desc) rn
                   from dim.urp_ngrs_channel_ref ) a
          where rn = 1) reff
                               on rech.ngrs_flag=reff.source_type_c and rech.upstream_name=reff.id
               left outer join (
          select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_pre_dd
          where event_date between ${start_date} and ${end_date}
          union all
          select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_post_dd
          where event_date between ${start_date} and ${end_date}) cb
                               on rech.bnum_msisdn=cb.msisdn and rech.event_date=cb.event_date
      where rech.event_date between ${start_date} and ${end_date}
      group by SUBSTRING(rech.trx_date,1,7),
               rech.trx_date,
               case
                   when (rech.ngrs_flag in('MKIOS','NM','M','','NA','N/A')
                       or rech.ngrs_flag is null) then 'TRADITIONAL'
                   when (rech.ngrs_flag in('URP','NU','U')
                       and reff.report_type is not null and upper(reff.report_type) in('DEALER','FISIK')) then 'TRADITIONAL'
                   else 'MODERN' end,
               case
                   when reff.report_type is not null then reff.report_type
                   when rech.ngrs_flag in ('URP','NU','U')
                       and reff.report_type is null then 'OTHERS'
                   when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name like '%ADOL%' then 'DEALER'
                   when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name not like '%ADOL%' then 'OUTLET'
                   else 'OTHERS ' end,
               case
                   when reff.channel_group is not null then reff.channel_group
                   when rech.ngrs_flag in ('URP','NU','U')
                       and reff.channel_group is null then 'OTHERS'
                   when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name like '%ADOL%' then 'DEALER'
                   when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name not like '%ADOL%' then 'OUTLET'
                   else 'OTHERS ' end,
               case
                   when reff.channel_category is not null then reff.channel_category
                   when rech.ngrs_flag in('URP','NU','U')
                       and reff.channel_category is null then 'OTHERS'
                   when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name like '%ADOL%' then 'AD ONLINE'
                   when (rech.ngrs_flag in('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name not like '%ADOL%' then 'AUTHORIZED DEALER'
                   else 'OTHERS ' end,
               case
                   when reff.channel_name is not null then reff.channel_name
                   when rech.ngrs_flag in ('URP','NU','U')
                       and reff.channel_name is null then 'OTHERS'
                   when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name like '%FINNETADOL%' then 'FINNET'
                   when (rech.ngrs_flag in ('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name like '%KISELADOL%' then 'KISEL'
                   when (rech.ngrs_flag in('MKIOS','NM','M','','NA','N/A') or rech.ngrs_flag is null)
                       and rech.upstream_name not like '%ADOL%' then 'OUTLET'
                   else 'OTHERS ' end,
               case
                   when rech.denom<5000 then '01. <5k'
                   when rech.denom=5000 then '02. 5k'
                   when rech.denom>5000 and rech.denom<10000 then '03. 6-9k'
                   when rech.denom=10000 then '04. 10k'
                   when rech.denom>10000 and rech.denom<20000 then '05. 11-19k'
                   when rech.denom=20000 then '06. 20k'
                   when rech.denom>20000 and rech.denom<25000 then '07. 21-24k'
                   when rech.denom=25000 then '08. 25k'
                   when rech.denom>25000 and rech.denom<50000 then '09. 26-49k'
                   when rech.denom=50000 then '10. 50k'
                   when rech.denom>50000 and rech.denom<100000 then '11. 51-99k'
                   when rech.denom=100000 then '12. 100k'
                   when rech.denom>100000 then '13. >100k'
                   else rech.denom end,
               rech.bnum_msisdn,
               cb.lacci) sop
         left outer join (
    select concat(lac,ci) lacci,
           site_id
    from dim.laccima_dim
    where lacci is not null and lacci not in ('null', 'UNKNOWN')
      and site_id is not null and site_id not in ('null', 'UNKNOWN')
      and event_date between ${start_date} and ${end_date}
    group by concat(lac,ci), site_id) site
                         on sop.lacci=site.lacci
group by sop.period,
         sop.trx_date,
         sop.report_group,
         sop.report_type,
         sop.channel_group,
         sop.channel_category,
         sop.channel_name,
         sop.denom_interval,
         sop.lacci,
         site.site_id;

--site_recharge_composition
select site_id,
       sum(case when report_group = 'TRADITIONAL' then total_amount else 0 end) rech_traditional,
       sum(case when report_type = 'OUTLET' then total_amount else 0 end) rech_outlet,
       sum(case when channel_name = 'ALFAMART' then total_amount else 0 end) rech_alfamart,
       sum(case when channel_name = 'INDOMARET' then total_amount else 0 end) rech_indomaret,
       sum(total_amount) total_rech
from nar.temp_ngk_optimumoutlet_rech_202201
where site_id is not null
group by site_id;

--site_profile
select site_id, kabupaten, region_sales, area_sales
from
    (select site_id, kabupaten, region_sales, area_sales, ROW_NUMBER() OVER(PARTITION BY site_id ORDER BY subs desc) AS rn
    from
        (select site_id, kabupaten, region_sales, area_sales, count(site_id) subs
         from dim.laccima_dim
         where event_date between ${start_date} and ${end_date}
           and site_id is not null
           and site_id not in ('null','UNKNOWN')
         group by site_id, kabupaten, region_sales, area_sales
        ) a
    ) a
where rn='1';