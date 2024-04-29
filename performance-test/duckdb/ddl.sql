drop table if exists tb_snapshot_sh_l2;
CREATE TABLE tb_snapshot_sh_l2 (
    securityid VARCHAR,
    tradetime TIMESTAMP,
    PreClosePx DOUBLE,
    OpenPx DOUBLE,
    HighPx DOUBLE,
    LowPx DOUBLE,
    LastPx DOUBLE,
    TotalVolumeTrade INTEGER,
    TotalValueTrade DOUBLE,
    InstrumentStatus VARCHAR,
    BidPrice0 DOUBLE,
    BidPrice1 DOUBLE,
    BidPrice2 DOUBLE,
    BidPrice3 DOUBLE,
    BidPrice4 DOUBLE,
    BidPrice5 DOUBLE,
    BidPrice6 DOUBLE,
    BidPrice7 DOUBLE,
    BidPrice8 DOUBLE,
    BidPrice9 DOUBLE,
    BidOrderQty0 INTEGER,
    BidOrderQty1 INTEGER,
    BidOrderQty2 INTEGER,
    BidOrderQty3 INTEGER,
    BidOrderQty4 INTEGER,
    BidOrderQty5 INTEGER,
    BidOrderQty6 INTEGER,
    BidOrderQty7 INTEGER,
    BidOrderQty8 INTEGER,
    BidOrderQty9 INTEGER,
    BidOrders0 INTEGER,
    BidOrders1 INTEGER,
    BidOrders2 INTEGER,
    BidOrders3 INTEGER,
    BidOrders4 INTEGER,
    BidOrders5 INTEGER,
    BidOrders6 INTEGER,
    BidOrders7 INTEGER,
    BidOrders8 INTEGER,
    BidOrders9 INTEGER,
    OfferPrice0 DOUBLE,
    OfferPrice1 DOUBLE,
    OfferPrice2 DOUBLE,
    OfferPrice3 DOUBLE,
    OfferPrice4 DOUBLE,
    OfferPrice5 DOUBLE,
    OfferPrice6 DOUBLE,
    OfferPrice7 DOUBLE,
    OfferPrice8 DOUBLE,
    OfferPrice9 DOUBLE,
    OfferOrderQty0 INTEGER,
    OfferOrderQty1 INTEGER,
    OfferOrderQty2 INTEGER,
    OfferOrderQty3 INTEGER,
    OfferOrderQty4 INTEGER,
    OfferOrderQty5 INTEGER,
    OfferOrderQty6 INTEGER,
    OfferOrderQty7 INTEGER,
    OfferOrderQty8 INTEGER,
    OfferOrderQty9 INTEGER,
    OfferOrders0 INTEGER,
    OfferOrders1 INTEGER,
    OfferOrders2 INTEGER,
    OfferOrders3 INTEGER,
    OfferOrders4 INTEGER,
    OfferOrders5 INTEGER,
    OfferOrders6 INTEGER,
    OfferOrders7 INTEGER,
    OfferOrders8 INTEGER,
    OfferOrders9 INTEGER,
    NumTrades INTEGER,
    IOPV DOUBLE,
    TotalBidQty INTEGER,
    TotalOfferQty INTEGER,
    WeightedAvgBidPx DOUBLE,
    WeightedAvgOfferPx DOUBLE,
    TotalBidNumber INTEGER,
    TotalOfferNumber INTEGER,
    BidTradeMaxDuration INTEGER,
    OfferTradeMaxDuration INTEGER,
    NumBidOrders INTEGER,
    NumOfferOrders INTEGER,
    WithdrawBuyNumber INTEGER,
    WithdrawBuyAmount INTEGER,
    WithdrawBuyMoney DOUBLE,
    WithdrawSellNumber INTEGER,
    WithdrawSellAmount INTEGER,
    WithdrawSellMoney DOUBLE,
    ETFBuyNumber INTEGER,
    ETFBuyAmount INTEGER,
    ETFBuyMoney DOUBLE,
    ETFSellNumber INTEGER,
    ETFSellAmount INTEGER,
    ETFSellMoney DOUBLE
);


drop index idx_securityid;
drop index idx_tradetime;
drop index idx_securityid_tradetime;


-- create table tb_test as from 'duckdb/testdata.csv'
-- select * from tb_test

-- copy tb_snapshot_sh_l2 from 'D:/Develop/DolphinDB_Win64_V3.00.0/server/testdata/test_*.csv' (header false)  -- 注意内存, test_*.csv爆内存了
copy tb_snapshot_sh_l2 from 'D:/Develop/DolphinDB_Win64_V3.00.0/server/testdata/*.csv' (header false)  -- 29 m 10 s 353 ms  --  37 m 38 s 544 ms

-- 创建索引
CREATE INDEX idx_securityid ON tb_snapshot_sh_l2 (securityid);
CREATE INDEX idx_tradetime ON tb_snapshot_sh_l2 (tradetime);
CREATE INDEX idx_securityid_tradetime ON tb_snapshot_sh_l2 (securityid, tradetime);
CREATE INDEX idx_tradetime_yymmdd ON tb_snapshot_sh_l2 (date_trunc('day', tradetime));
CREATE INDEX idx_tradetime_hhmmss ON tb_snapshot_sh_l2 (strftime(tradetime, '%X'));
SELECT * FROM duckdb_indexes;
