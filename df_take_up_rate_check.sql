select a.event_date,
       b.site_id,
       a.msisdn,
       a.content_id,
       a.package_group,
       a.package_subgroup,
       a.category,
       a.trx,
       a.rev
from (select a.trx_date event_date,
             b.lacci,
             a.msisdn,
             a.content_id,
             a.package_group,
             a.package_subgroup,
             a.category,
             sum(a.trx) trx,
             sum(a.revenue) rev
      from mkt_hq_v.bba_broadband_daily_202203 a
               left join (
          select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_pre_dd
          where event_date between ${start_date} and ${end_date}
          union all
          select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_post_dd
          where event_date between ${start_date} and ${end_date}
      ) b on a.msisdn=b.msisdn and a.trx_date=b.event_date
      where a.msisdn in (select msisdn from nar.temp_ngk_msisdn_piloting)
        and a.trx_date between ${start_date} and ${end_date}
      group by a.trx_date, b.lacci, a.msisdn, a.content_id, a.package_group, a.package_subgroup, a.category) a
         left join (
    select concat(lac,ci) lacci, site_id
    from dim.laccima_dim
    where lacci is not null and lacci not in ('null', 'UNKNOWN')
      and site_id is not null and site_id not in ('null', 'UNKNOWN')
      and event_date between ${start_date} and ${end_date}
    group by concat(lac,ci), site_id
) b on a.lacci=b.lacci
where b.site_id in ('CJR716', 'CJR605', 'BDS843', 'SMD174', 'CJR734', 'CJR366', 'BDB002', 'BDB314', 'BDS844');


select a.msisdn,
       b.site_id
from (
         select msisdn, event_date, concat(lac,ci) lacci
         from rna_all_v.cb_pre_dd
         where event_date between ${start_date} and ${end_date}
         union all
         select msisdn, event_date, concat(lac,ci) lacci from rna_all_v.cb_post_dd
         where event_date between ${start_date} and ${end_date}
     ) a
         left join (
    select concat(lac,ci) lacci, site_id
    from dim.laccima_dim
    where lacci is not null and lacci not in ('null', 'UNKNOWN')
      and site_id is not null and site_id not in ('null', 'UNKNOWN')
      and event_date between ${start_date} and ${end_date}
    group by concat(lac,ci), site_id
) b
                   on a.lacci=b.lacci
where a.msisdn in (select msisdn from nar.temp_ngk_msisdn_piloting)
group by msisdn, site_id;