-- 1建库 建表
-- drop database if exists db_k_day_level;
-- create database db_k_day_level;
-- use db_k_day_level;
--
-- DROP TABLE IF EXISTS tb_k_day;
-- CREATE TABLE tb_k_day (
--     securityid String,
--     tradetime Date,
--     open Float64,
--     close Float64,
--     high Float64,
--     low Float64,
--     vol Int32,
--     val Float64,
--     vwap Float64
-- ) ENGINE = MergeTree()
-- PARTITION BY toYYYYMMDD(tradetime)  -- 分区
-- ORDER BY securityid  -- 分区内部排序
-- SETTINGS index_granularity=8192;


drop database if exists db_snapshot_SH_L2_100G;
create database db_snapshot_SH_L2_100G;
use db_snapshot_SH_L2_100G;

DROP TABLE IF EXISTS tb_snapshot_SH_L2_MergeTree;
CREATE TABLE tb_snapshot_SH_L2_MergeTree (
    securityid String,
    tradetime DateTime,
    PreClosePx Float64,
    OpenPx Float64,
    HighPx Float64,
    LowPx Float64,
    LastPx Float64,
    TotalVolumeTrade Int32,
    TotalValueTrade Float64,
    InstrumentStatus String,
    BidPrice0 Float64,
    BidPrice1 Float64,
    BidPrice2 Float64,
    BidPrice3 Float64,
    BidPrice4 Float64,
    BidPrice5 Float64,
    BidPrice6 Float64,
    BidPrice7 Float64,
    BidPrice8 Float64,
    BidPrice9 Float64,
    BidOrderQty0 Int32,
    BidOrderQty1 Int32,
    BidOrderQty2 Int32,
    BidOrderQty3 Int32,
    BidOrderQty4 Int32,
    BidOrderQty5 Int32,
    BidOrderQty6 Int32,
    BidOrderQty7 Int32,
    BidOrderQty8 Int32,
    BidOrderQty9 Int32,
    BidOrders0 Int32,
    BidOrders1 Int32,
    BidOrders2 Int32,
    BidOrders3 Int32,
    BidOrders4 Int32,
    BidOrders5 Int32,
    BidOrders6 Int32,
    BidOrders7 Int32,
    BidOrders8 Int32,
    BidOrders9 Int32,
    OfferPrice0 Float64,
    OfferPrice1 Float64,
    OfferPrice2 Float64,
    OfferPrice3 Float64,
    OfferPrice4 Float64,
    OfferPrice5 Float64,
    OfferPrice6 Float64,
    OfferPrice7 Float64,
    OfferPrice8 Float64,
    OfferPrice9 Float64,
    OfferOrderQty0 Int32,
    OfferOrderQty1 Int32,
    OfferOrderQty2 Int32,
    OfferOrderQty3 Int32,
    OfferOrderQty4 Int32,
    OfferOrderQty5 Int32,
    OfferOrderQty6 Int32,
    OfferOrderQty7 Int32,
    OfferOrderQty8 Int32,
    OfferOrderQty9 Int32,
    OfferOrders0 Int32,
    OfferOrders1 Int32,
    OfferOrders2 Int32,
    OfferOrders3 Int32,
    OfferOrders4 Int32,
    OfferOrders5 Int32,
    OfferOrders6 Int32,
    OfferOrders7 Int32,
    OfferOrders8 Int32,
    OfferOrders9 Int32,
    NumTrades Int32,
    IOPV Float64,
    TotalBidQty Int32,
    TotalOfferQty Int32,
    WeightedAvgBidPx Float64,
    WeightedAvgOfferPx Float64,
    TotalBidNumber Int32,
    TotalOfferNumber Int32,
    BidTradeMaxDuration Int32,
    OfferTradeMaxDuration Int32,
    NumBidOrders Int32,
    NumOfferOrders Int32,
    WithdrawBuyNumber Int32,
    WithdrawBuyAmount Int32,
    WithdrawBuyMoney Float64,
    WithdrawSellNumber Int32,
    WithdrawSellAmount Int32,
    WithdrawSellMoney Float64,
    ETFBuyNumber Int32,
    ETFBuyAmount Int32,
ETFBuyMoney Float64,
ETFSellNumber Int32,
ETFSellAmount Int32,
ETFSellMoney Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(tradetime)
-- PARTITION BY (toYYYYMMDD(tradetime), securityid)  -- 分区
ORDER BY (securityid, tradetime)  -- 分区内部排序
SETTINGS index_granularity=8192;

-- 2.导入数据
-- docker cp D:/Develop/DolphinDB_Win64_V3.00.0/server/testdata clickhouse-server:/data/testdata
-- docker exec -it clickhouse-server bash
-- bash -v ./batch-insert.sh
