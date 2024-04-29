use db_snapshot_SH_L2_100G;
-- use db_snapshot_SH_L2;

-- 查询总数
SELECT count(*) FROM tb_snapshot_SH_L2_MergeTree;

-- 点查询
SELECT * FROM tb_snapshot_SH_L2_MergeTree
WHERE securityid = 'sz002582'
AND tradetime = '2020-01-02 11:30:00';

-- 范围查询
SELECT securityid, tradetime, OpenPx, PreClosePx FROM tb_snapshot_SH_L2_MergeTree
WHERE securityid IN ('sz002582', 'sz001900', 'sz003183', 'sz000002')
AND DATE(tradetime) between '2020-01-01' and '2020-01-02'
AND PreClosePx < 60;

-- top1000+排序
SELECT * FROM tb_snapshot_SH_L2_MergeTree
WHERE securityid IN ('sz000001', 'sz000002', 'sz000003', 'sz000004')
AND formatDateTime(tradetime, '%T') between '09:30:00' AND '11:30:00'
AND OpenPx > PreClosePx
ORDER BY (OpenPx - PreClosePx) DESC LIMIT 1000;

-- 聚合查询：单分区维度
SELECT max(OpenPx) as max_OpenPx, min(PreClosePx) as min_PreClosePx FROM tb_snapshot_SH_L2_MergeTree
WHERE securityid = 'sz001664'
AND OpenPx > PreClosePx
GROUP BY toMonth(tradetime);

-- 聚合查询：多分区维度 + 排序
SELECT securityid, month_time, stddevPop(BidPrice0) as std_bid, sum(BidOrderQty0) as sum_BidOrderQty FROM tb_snapshot_SH_L2_MergeTree
where securityid IN ('sz000922', 'sz003925', 'sz000505', 'sz002435')
and tradetime >= '2020-01-01 09:30:00'
group by securityid, toMonth(tradetime) as month_time
order by securityid asc;

-- 经典查询：按 [多个股票代码、日期，时间范围、报价范围] 过滤，查询 [股票代码、时间、卖方与买方报价]
SELECT * FROM tb_snapshot_SH_L2_MergeTree
where securityid IN ('sz002435', 'sz002436', 'sz002437', 'sz002438')
AND tradetime >= '2020-01-01 09:30:00'
AND tradetime <= '2020-01-02 11:30:00'
AND BidPrice0 > 60
AND OfferPrice0 > BidPrice0;

-- 窗口计算：查询某只股票某天内时间的差值
SELECT securityid, tradetime, runningDifference(BidPrice0) AS BidPrice0_diff FROM tb_snapshot_SH_L2_MergeTree
WHERE securityid = 'sz002436' AND DATE(tradetime) = '2020-01-02'
ORDER BY tradetime ASC;

-- 聚合计算：计算某天中每个股票在每分钟的最大卖方报价与最小买方报价之差
SELECT max(OfferPrice0) - min(BidPrice0) AS gap FROM tb_snapshot_SH_L2_MergeTree
WHERE DATE(tradetime) = '2020-01-01'
AND BidPrice0 > 0
AND OfferPrice0 > BidPrice0
GROUP BY securityid, formatDateTime(tradetime, '%R') AS minute;

-- 统计计算：计算某天中某列的中位数（ClickHouse查询median时是近似结果，而DolphinDB是精确结果）
SELECT median(OfferPrice0), median(BidPrice0) FROM tb_snapshot_SH_L2_MergeTree
WHERE DATE(tradetime) = '2020-01-02'
AND securityid = 'sz002438';


-- STOP

select max((OfferPrice0-BidPrice0)/(OfferPrice0+BidPrice0)*2) as avgSpread
from tb_snapshot_SH_L2_MergeTree
where securityid = 'sz000001'
and tradetime between '2020-01-03 09:30:00' and '2020.01.03 15:00:00'
and OfferPrice0>=BidPrice0 group by toStartOfMinute(tradetime) as minute;

select formatDateTime(tradetime, "%T")
from tb_snapshot_SH_L2_MergeTree
limit 1;

-- 还支持http的async_insert
-- 插入数据 有序
insert into tb_snapshot_SH_L2_MergeTree (securityid, tradetime, PreClosePx) values ('sz004001', '2020-01-07 18:00:00', 0);
-- 插入数据 无序
insert into tb_snapshot_SH_L2_MergeTree (PreClosePx) values (0);

-- 删除数据

