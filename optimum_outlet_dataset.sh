today=$(date +%Y-%m-%d)
m1=$(date --date='1 months ago' +%Y-%m-%d)
m2=$(date --date='2 months ago' +%Y-%m-%d)

leap_m1=$((${m1:0:4}%4))
last_day_m1=$(date -d "${m1:5:2}/1 + 1 month - 1 day" "+%d")

leap_m2=$((${m2:0:4}%4))
last_day_m2=$(date -d "${m2:5:2}/1 + 1 month - 1 day" "+%d")

if [ $leap_m1 == 0 ] && [ ${m1:5:2} == 02 ]
then
last_day_m1=$((last_day_m1+1))
fi

if [ $leap_m2 == 0 ] && [ ${m2:5:2} == 02 ]
then
last_day_m2=$((last_day_m2+1))
fi

event_date_start=${m1:0:8}"01"
event_date_end=${m1:0:8}${last_day_m1}

beeline -u 'jdbc:hive2://bihdpapp001.telkomsel.co.id:10000/default;principal=hive/bihdpapp001@TELKOMSEL.CO.ID' \
--outputformat=csv2 \
--hiveconf mapreduce.job.queuename=root.hui_marketing \
--hiveconf hive.groupby.orderby.position.alias=true \
--hiveconf hive.exec.dynamic.partition.mode=nonstrict \
--hiveconf hive.exec.dynamic.partition=true \
--hiveconf hive.map.aggr=true \
--hiveconf hive.merge.mapfiles=true \
--hiveconf hive.merge.mapredfiles=true \
--hiveconf hive.merge.size.per.task=268435456 \
--hiveconf hive.merge.smallfiles.avgsize=134217728 \
--hiveconf hive.exec.compress.output=true \
--hiveconf mapreduce.map.memory.mb=4056 \
--hiveconf mapreduce.map.java.opts=-Xmx3600m \
--hiveconf mapreduce.reduce.memory.mb=4505 \
--hiveconf mapreduce.reduce.java.opts=-Xmx4055m \
--hiveconf hive.exec.reducers.bytes.per.reducer=1073741824 \
--hiveconf fs.blocksize=1073741824 \
--hiveconf hive.vectorized.execution.enabled = false \
--hiveconf hive.vectorized.execution.reduce.enabled = false \
-e \
"
select '${event_date_end:0:4}${event_date_end:5:2}' periode, a.site_id site_id, archetype,
    coalesce(outlet_num,0) outlet_num,
    coalesce(active_outlet_num,0) active_outlet_num,
    coalesce(trx_active_traditional_channel,0) trx_active_traditional_channel,
    coalesce(rev_active_traditional_channel,0) rev_active_traditional_channel,
    kabupaten, region_sales, area_sales
from
(
    select site_id, archetype
    from ppm.regha_saf_site_4g_archetype_202110
) a
left join
(
    select site_id,
        count(a.outlet_id) outlet_num,
        count(case when (coalesce(trx_recharge_reguler,0)+coalesce(trx_recharge_sa,0)+coalesce(trx_recharge_vas,0)+coalesce(trx_physical_voucher,0))>=2 then a.outlet_id end) active_outlet_num,
        sum(case when (coalesce(trx_recharge_reguler,0)+coalesce(trx_recharge_sa,0)+coalesce(trx_recharge_vas,0)+coalesce(trx_physical_voucher,0))>=2 then coalesce(trx_recharge_reguler,0)+coalesce(trx_recharge_sa,0)+coalesce(trx_recharge_vas,0)+coalesce(trx_physical_voucher,0) else 0 end) trx_active_traditional_channel,
        sum(case when (coalesce(trx_recharge_reguler,0)+coalesce(trx_recharge_sa,0)+coalesce(trx_recharge_vas,0)+coalesce(trx_physical_voucher,0))>=2 then coalesce(recharge_reguler,0)+coalesce(recharge_sa,0)+coalesce(recharge_vas,0)+coalesce(physical_voucher,0) else 0 end) rev_active_traditional_channel
    from
    (
        select site_id, outlet_id
        from ppm.regha_saf_site_id_outlet_voronoi_reff
        group by 1,2
    ) a
    left join
    (
        -- Recharge Reguler
        select outlet_id, count(outlet_id) trx_recharge_reguler, sum(denom) as recharge_reguler
        from base.digipos_recharge
        where event_date between '${event_date_start}' and '${event_date_end}'
        group by 1
    ) b
    on a.outlet_id=b.outlet_id
    left join
    (
        -- Recharge SA
        select outlet_id, count(outlet_id) trx_recharge_sa, sum(price) as recharge_sa
        from base.digipos_nsb_package
        where event_date between '${event_date_start}' and '${event_date_end}'
        group by 1
    ) c
    on a.outlet_id=c.outlet_id
    left join
    (
        -- Recharge VAS Lengkap
        select outlet_id, count(outlet_id) as trx_recharge_vas, sum(price) as recharge_vas
        from base.digipos_package_activation
        where event_date between '${event_date_start}' and '${event_date_end}'
        group by 1
    ) d
    on a.outlet_id=d.outlet_id
    left join
    (
        -- Recharge Physical Voucher
        select outlet_id, count(outlet_id) trx_physical_voucher, sum(denom) as physical_voucher
        from
        (
            select outlet_id, c.msisdn no_rs, denom
            from
            (    
                select outlet_id, no_rs, denom
                from base.digipos_physical_voucher
                where event_date between '${event_date_start}' and '${event_date_end}'
            ) a
            inner join
            (
                select tokenized_msisdn, msisdn,
                    case
                    when substring(msisdn,1,1)='0' then concat('62', substring(msisdn, 2, length(msisdn)))
                    when substring(msisdn,1,1)='8' then concat('62', msisdn)
                    else msisdn
                    end new_msisdn
                from base.cache_msisdn a
                where a.event_date in (select max(event_date) from base.cache_msisdn)
                group by 1,2
            ) c
            on a.no_rs=c.tokenized_msisdn
        ) a
        left join
        (
            select no_rs
            from sch.rs_sa_dealer
            group by 1
        ) b
        on a.no_rs=b.no_rs
        where b.no_rs is null
        group by 1
    ) e
    on a.outlet_id=e.outlet_id
    group by 1
) b
on a.site_id=b.site_id
left join
(
    select site_id, kabupaten, region_sales, area_sales
    from
    (
        select site_id, kabupaten, region_sales, area_sales, ROW_NUMBER() OVER(PARTITION BY site_id ORDER BY subs desc) AS rn
        from
        (
            select site_id, kabupaten, region_sales, area_sales, count(site_id) subs
            from dim.laccima_dim
            where event_date between '${m2:0:8}01' and '${event_date_end}'
            and site_id is not null
            and site_id not in ('null','UNKNOWN')
            group by 1,2,3,4
        ) a
    ) a
    where rn='1'
) c
on a.site_id=c.site_id;
" \
> /abusers/tsel/g_postpaid/reghapra/saf/optimum_outlet/optimum_outlet_dataset_${event_date_end:0:4}${event_date_end:5:2}.csv \
2>> /abusers/tsel/g_postpaid/reghapra/saf/optimum_outlet/optimum_outlet_dataset_${event_date_end:0:4}${event_date_end:5:2}.log

gzip /abusers/tsel/g_postpaid/reghapra/saf/optimum_outlet/optimum_outlet_dataset_${event_date_end:0:4}${event_date_end:5:2}.csv
