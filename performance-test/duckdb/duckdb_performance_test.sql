-- 查询总数
SELECT count(*) FROM tb_snapshot_sh_l2;
-- 58 ms

-- 点查询
SELECT * FROM tb_snapshot_sh_l2
WHERE securityid = 'sz002582'
AND tradetime = '2020-01-02 11:30:00';

-- 范围查询
SELECT securityid, tradetime, OpenPx, PreClosePx FROM tb_snapshot_sh_l2
WHERE securityid IN ('sz002582', 'sz001900', 'sz003183', 'sz000002')
AND date_trunc('day', tradetime) between '2020-01-01' and '2020-01-02'
AND PreClosePx < 60;
-- cast(tradetime as DATE)


-- top1000+排序
SELECT * FROM tb_snapshot_sh_l2
WHERE securityid IN ('sz000001', 'sz000002', 'sz000003', 'sz000004')
AND strftime(tradetime, '%X') between '09:30:00' AND '11:30:00'
AND OpenPx > PreClosePx
ORDER BY (OpenPx - PreClosePx) DESC LIMIT 1000;
-- >40min

-- 聚合查询：单分区维度
SELECT max(OpenPx) as max_OpenPx, min(PreClosePx) as min_PreClosePx FROM tb_snapshot_sh_l2
WHERE securityid = 'sz001664'
AND OpenPx > PreClosePx
GROUP BY strftime('%Y-%m', tradetime);
-- 在 102 ms 106 ms 95 ms 161 ms

-- 聚合查询：多分区维度 + 排序
SELECT securityid, month(tradetime), stddev_pop(OpenPx) as std_open, sum(BidOrderQty0) as sum_BidOrderQty FROM tb_snapshot_sh_l2
where securityid IN ('sz000922', 'sz003925', 'sz000505', 'sz002435')
and tradetime >= '2020-01-01 09:30:00'
group by securityid, month(tradetime)
order by securityid asc;
-- 204 ms 189 ms 169 ms 110 ms

-- 经典查询：按 [多个股票代码、日期，时间范围、报价范围] 过滤，查询 [股票代码、时间、卖方与买方报价]
SELECT securityid, tradetime, BidPrice0, OfferPrice0s FROM tb_snapshot_sh_l2
where securityid IN ('sz002435', 'sz002436', 'sz002437', 'sz002438')
AND tradetime >= '2020-01-01 09:30:00'
AND tradetime <= '2020-01-02 11:30:00'
AND BidPrice0 > 60
AND OfferPrice0s > BidPrice0;
-- 176 ms 133 ms 105 ms 182 ms

-- 窗口计算：查询某只股票某天内时间的差值
SELECT securityid, tradetime, BidPrice0 - lag(BidPrice0) OVER (ORDER BY tradetime) AS time_diff
FROM tb_snapshot_sh_l2
WHERE securityid = 'sz002436' AND date_trunc('day', tradetime) = '2020-01-02'
ORDER BY tradetime ASC;

-- 聚合计算：计算某天中每个股票在每分钟的最大卖方报价与最小买方报价之差
SELECT securityid, max(OfferPrice0s) - min(BidPrice0) AS gap FROM tb_snapshot_sh_l2
WHERE date_trunc('day', tradetime) = '2020-01-01'
AND BidPrice0 > 0
AND OfferPrice0s > BidPrice0
GROUP BY securityid, strftime('%I:%M', tradetime);
-- 725 ms  703 ms 709 ms 717 ms

-- 统计计算：计算某天中某列的中位数
SELECT median(OfferPrice0s), median(BidPrice0) FROM tb_snapshot_sh_l2
WHERE date_trunc('day', tradetime) = '2020-01-02'
AND securityid = 'sz002438';


select strftime('%I:%M', tradetime), max((OfferPrice0s-BidPrice0)/(OfferPrice0s+BidPrice0)*2) as avgSpread from tb_snapshot_sh_l2
where securityid = 'sz000001'
AND tradetime between '2020-01-03 09:30:00.000' AND '2020-01-03 15:00:00.000'
AND OfferPrice0s >= BidPrice0
group by ANY_VALUE(tradetime), strftime('%I-%M', tradetime)


insert into tb_snapshot_sh_l2 (securityid, tradetime, PreClosePx) values ('sz004001', '2020-01-07 18:00:00', 0);

select * from tb_snapshot_sh_l2 limit 1 OffSET 192080003;