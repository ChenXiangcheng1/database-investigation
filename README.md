# 时序数据库调研

## 基本

|              | DolphinDB                                                    | ClickHouse                                                  | DuckDB               | InfluxDB                        |
| ------------ | ------------------------------------------------------------ | ----------------------------------------------------------- | -------------------- | ------------------------------- |
| 当前版本     | dolphindb:v3.00.0s                                           | clickhouse-server:24.3.2.23                                 | v0.12.2              | influxdb:2.7.6                  |
| 开源情况     | 社区版(限制内存8G 2Core)/闭源企业版收费                      | 开源                                                        | 开源                 | 开源版(单机)/商业版(分布式集群) |
| 存储         | 列式存储、分区                                               | 列式存储、分区                                              | 列式存储、单文件     |                                 |
| 事务         | 支持事务                                                     | 不支持事务                                                  | 支持事务             |                                 |
| 主要存储引擎 | OLAP(默认)、TSDB                                             | 表引擎多样化主要分为MergeTree、日志、外部集成引擎 、其他    |                      |                                 |
| 使用         | DolphinDB Script、Python API                                 | SQL-Like、有Python驱动clickhouse_driver、clickhouse_connect | SQL-Like、Python API | InfluxQL                        |
| 分布式       | 支持分布式集群                                               | 支持分布式集群                                              | 不支持               |                                 |
| 应用场景     | 金融领域OLAP 可以处理高频数据<br />不仅是数据库还内置MapReduce等多种计算框架、流处理引擎能完成计算任务 | 宽表 OLAP 离线数据分析                                      | 嵌入式、需要轻量级   | OLAP                            |
| 背书         | 部分券商和私募使用                                           | 很多大公司背书                                              |                      | 开源时序认可度最高( DB-Engine)  |

BTW：还看到有些用 [HDF5](https://zhuanlan-zhihu-com.translate.goog/p/675563949?_x_tr_sl=zh-CN&_x_tr_tl=en&_x_tr_hl=en&_x_tr_pto=sc) 文件系统的
[h5py](https://www.h5py.org/)





## 性能测试

数据库：社区单机版 DolphinDB，单机版 ClickHouse，单机版 DuckDB



### 数据集

数据集： 随机生成的模拟 level2 快照数据，共192,080,000条记录，146G CSV

DDB分区键：tradetime、securityid
DDB排序键：securityid、tradetime

ClickHouse分区键：tradetime
ClickHouse排序键：securityid、tradetime

DuckDB设置了索引 (securityid)、(tradetime)、(securityid, tradetime)、date_trunc('day', tradetime)、strftime(tradetime, '%X')

[Clickhouse 建库建表](./performance-test/ch/ddl.sql)	|	[DDB 建库建表](./performance-test/ddb/createSnapshotDbTable.dos)	|	[DuckDB 建库建表](./performance-test/duckdb/ddl.sql)





### 查询

| Query                       | DDB                                                          | CH                                                           | DuckDB                                                       |
| --------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 查询总数                    | `select count(*) from tb`                                    | `SELECT count(*) FROM tb_snapshot_SH_L2_MergeTree;`          | `SELECT count(*) FROM tb_snapshot_sh_l2;`                    |
| 点查询                      | `select * from tb where securityid = 'sz002582', tradetime = 2020.01.02 11:30:00` | `SELECT * FROM tb_snapshot_SH_L2_MergeTree WHERE securityid = 'sz002582' AND tradetime = '2020-01-02 11:30:00';` | `SELECT * FROM tb_snapshot_sh_l2 WHERE securityid = 'sz002582' AND tradetime = '2020-01-02 11:30:00';` |
| 范围查询                    | `select securityid, tradetime, OpenPx, PreClosePx from tb  where securityid in ('sz002582', 'sz001900', 'sz003183', 'sz000002'), date(tradetime) between 2020.01.01 : 2020.01.02, PreClosePx < 60` | `SELECT securityid, tradetime, OpenPx, PreClosePx FROM tb_snapshot_SH_L2_MergeTree WHERE securityid IN ('sz002582', 'sz001900', 'sz003183', 'sz000002') AND DATE(tradetime) between '2020-01-01' and '2020-01-02' AND PreClosePx < 60;` | `SELECT securityid, tradetime, OpenPx, PreClosePx FROM tb_snapshot_sh_l2 WHERE securityid IN ('sz002582', 'sz001900', 'sz003183', 'sz000002') AND date_trunc('day', tradetime) between '2020-01-01' and '2020-01-02' AND PreClosePx < 60;` |
| top1000+排序                | `select top 1000 * from tb where securityid in ('sz000001', 'sz000002', 'sz000003', 'sz000004'), time(tradetime) >= time(09:30:000), time(tradetime) <= time(11:30:00.000),  OpenPx > PreClosePx order by (OpenPx - PreClosePx) desc` | `SELECT * FROM tb_snapshot_SH_L2_MergeTree WHERE securityid IN ('sz000001', 'sz000002', 'sz000003', 'sz000004') AND formatDateTime(tradetime, '%T') between '09:30:00' AND '11:30:00' AND OpenPx > PreClosePx ORDER BY (OpenPx - PreClosePx) DESC LIMIT 1000;` | `SELECT * FROM tb_snapshot_sh_l2 WHERE securityid IN ('sz000001', 'sz000002', 'sz000003', 'sz000004') AND strftime(tradetime, '%X') between '09:30:00' AND '11:30:00' AND OpenPx > PreClosePx ORDER BY (OpenPx - PreClosePx) DESC LIMIT 1000;` |
| 聚合查询：单分区维度        | `select max(OpenPx) as max_OpenPx, min(PreClosePx) as min_PreClosePx from tb where securityid = 'sz001664', OpenPx > PreClosePx group by month(tradetime)` | `SELECT max(OpenPx) as max_OpenPx, min(PreClosePx) as min_PreClosePx FROM tb_snapshot_SH_L2_MergeTree WHERE securityid = 'sz001664' AND OpenPx > PreClosePx GROUP BY toMonth(tradetime);` | `SELECT max(OpenPx) as max_OpenPx, min(PreClosePx) as min_PreClosePx FROM tb_snapshot_sh_l2 WHERE securityid = 'sz001664' AND OpenPx > PreClosePx GROUP BY strftime('%Y-%m', tradetime);` |
| 聚合查询：多分区维度 + 排序 | `select std(OpenPx) as std_open, sum(BidOrderQty0) as   from tb where securityid in ('sz000922', 'sz003925', 'sz000505', 'sz002435'), tradetime >= timestamp("2020.01.01 09:30:00") group by securityid, month(tradetime) order by securityid asc` | `SELECT securityid, month_time, stddevPop(BidPrice0) as std_bid, sum(BidOrderQty0) as sum_BidOrderQty FROM tb_snapshot_SH_L2_MergeTree where securityid IN ('sz000922', 'sz003925', 'sz000505', 'sz002435') and tradetime >= '2020-01-01 09:30:00' group by securityid, toMonth(tradetime) as month_time order by securityid asc;` | `SELECT securityid, month(tradetime), stddev_pop(OpenPx) as std_open, sum(BidOrderQty0) as sum_BidOrderQty FROM tb_snapshot_sh_l2 where securityid IN ('sz000922', 'sz003925', 'sz000505', 'sz002435') and tradetime >= '2020-01-01 09:30:00' group by securityid, month(tradetime) order by securityid asc;` |
| 经典查询                    | `select * from tb where securityid in ('sz002435', 'sz002436', 'sz002437', 'sz002438'), tradetime >= timestamp("2020.01.01 09:30:00"), tradetime <= timestamp("2020.01.02 11:30:00"), BidPrice0 > 60, OfferPrice0 > BidPrice0` | `SELECT * FROM tb_snapshot_SH_L2_MergeTree where securityid IN ('sz002435', 'sz002436', 'sz002437', 'sz002438') AND tradetime >= '2020-01-01 09:30:00' AND tradetime <= '2020-01-02 11:30:00' AND BidPrice0 > 60 AND OfferPrice0 > BidPrice0;` | `SELECT securityid, tradetime, BidPrice0, OfferPrice0s FROM tb_snapshot_sh_l2 where securityid IN ('sz002435', 'sz002436', 'sz002437', 'sz002438') AND tradetime >= '2020-01-01 09:30:00' AND tradetime <= '2020-01-02 11:30:00' AND BidPrice0 > 60 AND OfferPrice0s > BidPrice0;` |



| Query                                                        | DDB第一次 | 2      | 3      | CH第一次  | 2         | 3         | DuckDB第一次   | 2           | 3           |
| ------------------------------------------------------------ | --------- | ------ | ------ | --------- | --------- | --------- | -------------- | ----------- | ----------- |
| 查询总数                                                     | 2.7 s     | 234 ms | 238 ms | 240 ms    | 28 ms     | 32 ms     | 603 ms         | 49 ms       | 46 ms       |
| 点查询                                                       | 37 ms     | 6 ms   | 11 ms  | 177 ms    | 122 ms    | 111 ms    | 2s 371 ms      | 122 ms      | 105 ms      |
| 范围查询                                                     | 226 ms    | 17 ms  | 12 ms  | 1s 235 ms | 799 ms    | 712 ms    | 2 m 50 s 95 ms | 890 ms      | 837 ms      |
| top1000+排序                                                 | 4.0 s     | 243 ms | 246 ms | 519 ms    | 241 ms    | 225 ms    | 大于 40 m      |             |             |
| 聚合查询：单分区维度                                         | 194 ms    | 5 ms   | 5 ms   | 49 ms     | 33 ms     | 32 ms     | 5s 583 ms      | 369 ms      | 415 ms      |
| 聚合查询：多分区维度 + 排序                                  | 918 s     | 19 ms  | 18 ms  | 377 ms    | 42 ms     | 51 ms     | 1m 47s 913 mss | 57 s 554 ms | 40 s 236 ms |
| 经典查询<br />按 [多个股票代码、日期，时间范围、报价范围] 过滤 | 344 ms    | 38 ms  | 43 ms  | 414 ms    | 346 ms    | 316 ms    | 27 s 27ms      | 11 s 339 ms | 12 s 391 ms |
| 窗口计算<br />查询某只股票某天内时间的差值                   | 43 ms     | 3 ms   | 7 ms   | 255 ms    | 156 ms    | 161 ms    | 5 s 610 ms     | 89 ms       | 81 ms       |
| 聚合计算<br />计算某天中每个股票在每分钟的最大卖方报价与最小买方报价之差 | 3min 11 s | 1.7 s  | 1.6 s  | 4s 79 ms  | 3s 493 ms | 3s 630 ms | 2m 50 s 888 ms | 4s 696 ms   | 4s 575 ms   |
| 统计计算<br />计算某天中某列的中位数                         | 25 ms     | 2 ms   | 2 ms   | 331 ms    | 37 ms     | 35 ms     | 51 ms          | 43 ms       | 44 ms       |

[Clickhouse 性能测试](./performance-test/ch/performance_test.sql)	|	[DDB 性能测试](./performance-test/ddb/performance_test.dos)	|	[DuckDB 性能测试](./performance-test/duckdb/duckdb_performance_test.sql)





### 更新和删除

* DDB 的更新记录**是将整个chunk块或表删除后更新**而不是行更新，仅适用于低频更新，不适合高频更新(毫秒级更新任务)，所以测试也没意义

* **Clickhouse 不支持行记录的更新和删除** update delete，如果要实现"行更新/删除" 要更新/删除整个分区 [alter table](https://clickhouse.com/docs/en/sql-reference/statements/alter)

  ```sql
  alter table mutations_operate delete where toYYYYMMDD(CreateTime) = 20230808 and UserId between 1000 and 10000;  -- 该操作是异步的
  ```

* DuckDB支持行记录的更新





### 插入

列式数据库，建议采用批量插入，并且插入之前预先对数据分组

| 插入三条连续记录 | DDB 第一条2 | 2      | 3      | Clickhouse | 2     | 3     | DuckDB     | 2      | 3     |
| ---------------- | ----------- | ------ | ------ | ---------- | ----- | ----- | ---------- | ------ | ----- |
|                  | 893 ms      | 150 ms | 139 ms | 49 ms      | 11 ms | 18 ms | 8 s 417 ms | 118 ms | 94 ms |

BTW： DDB、Clickhouse设置排序键，插入后数据有序。DuckDB创建表时不能指定排序方式，插入后无序，因此插入数据后，每次查询需要order by，可以创建视图View





### CSV导入数据库

DDB 社区版限制了内存8G，需要分块导出

|      | DolphinDB  | Clickhouse | DuckDB     |
| ---- | ---------- | ---------- | ---------- |
| 3.5G | 1 min 22 s | 45 s       |            |
| 100G |            | 1h 36m     | 1h 15m 42s |

BTW：内存、硬盘也是瓶颈，会吃满内存、硬盘活动时间





### alpha因子挖掘

[alpha因子总结](http://www.qianshancapital.com/h-nd-329.html)
alpha因子挖掘：可以从数据库中取数据回Pandas/Numpy做、一些第三方库[alphalens(不支持最新的pandas)](https://github.com/quantopian/alphalens)做、通过DDB做、放到clickhouse-server端做

DDB支持alpha因子的挖掘，这里对比下DDB和pandas效率
数据集：随机生成的10年的模拟日K数据，共10,432,000条记录，2.5G CSV

| alphaNames | ddb 毫秒 | pandas 毫秒        |
| ---------- | -------- | ------------------ |
| 1          | 304      | 55478.6319732666   |
| 2          | 253      | 1770.2686786651611 |
| 3          | 126      | 1787.1625423431396 |
| 4          | 83       | 173826.07007026672 |
| 5          | 107      | 514.2631530761719  |
| 6          | 25       | 1612.2708320617676 |
| 7          | 123      | 187701.33709907532 |
| 8          | 96       | 1046.8714237213135 |
| 9          | 34       | 516.669511795044   |
| 10         | 100      | 689.9755001068115  |
| 11         | 117      | 788.7811660766602  |
| 12         | 10       | 28.16319465637207  |
| 13         | 186      | 1860.6712818145752 |
| 14         | 95       | 1725.6314754486084 |
| 15         | 205      | 2191.636323928833  |
| 16         | 191      | 1821.3181495666504 |
| 17         | 151      | 349700.00886917114 |
| 18         | 93       | 1874.9327659606934 |
| 19         | 51       | 354.34556007385254 |
| 20         | 221      | 453.9988040924072  |
| ...        | ...      | ...                |

[DDB代码](./WorldQuant 101 Alpha 因子指标库/time-test/wq101alphaDDBTime.dos)				|	[Pandas代码](./WorldQuant 101 Alpha 因子指标库/time-test/wq101alphaPyTime.py)
[DDB因子测试完整结果](./WorldQuant 101 Alpha 因子指标库/res/ddbPerformance2.csv)	|	[Pandas因子测试完整结果](./WorldQuant 101 Alpha 因子指标库/res/pyPerformance.txt)





## 易用性

维护：

* DDB 封装了 DFS 分布式文件系统，对开发者透明，分布式表好维护
  DDB不仅是数据库还提供了一站式解决方案，有一些结合业务的问题也容易通过DDB文档解决
  商业版提供技术支持
* Clickhouse如果要部署集群，还需要 Zookeeper 管理元数据，如果要实时还要结合Apache Flink流处理框架
* DuckDB轻量级，不支持表分区，要手动管理
  DuckDB稳定性不行，跑一个SQL再中止执行，结果数据库文件出问题了之后无法连接上
  DuckDB避免全表扫描的方式是索引，而DDB、Clickhouse是通过分区，DuckDB 写SQL还需要注意尽量走索引，不如DDB、CH易用

使用：

* DDB 开发了自己的脚本语言，提供 Python API 客户端、支持的算子很多、官方提供一站式解决方案
  Python API 就是在他自己脚本语言上封装的，感觉DDB Script 比Python API方便
  DolphinDB脚本的文档更全面而且支持多种编程范式命令式、SQL等
  DolphinDB不仅是数据库，还集成了MapReduce计算框架、流计算引擎，例下可以执行map-reduce操作，执行一些计算任务
  
  ```dos
  def main(dbName,tableName,days){
      // rds = sqlDS(<select * from loadTable("dfs://level2","quotes") where date=2020.06.01>)  // 之前哈希分区10个，所以返回10个数据源
  	rds = sqlDS(<select * from loadTable(dbName,tableName) where date=2020.06.01>)
  	mr(ds=rds, mapFunc=calcData{,dbName,tableName,days}, parallel=true)  // 参数：数据源、函数、是否并行
  }
  ```
* Clickhouse 有官方维护的python驱动[clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect)和第三方的Python驱动[clickhouse-drive](https://github.com/mymarilyn/clickhouse-driver) 来建立连接、执行SQL
  其中query相关方法能返回Numpy数组、Pandas Dataframe
* DuckDB提供Python API 客户端，Pandas DataFrames 与 DuckDB SQL 查询可以无缝集成，即 `SELECT * FROM my_dataframe`

  ```python
  import duckdb
  import pandas
  
  # Create a Pandas dataframe
  my_df = pandas.DataFrame.from_dict({'a': [42]})
  
  # query the Pandas DataFrame "my_df"
  # Note: duckdb.sql connects to the default in-memory database connection
  results = duckdb.sql("SELECT * FROM my_df").df()
  ```

  

### SQL

* DDB 支持很多拓展的SQL子句、计算函数

  * 支持数组类型、嵌套类型
  * exec：返回一个值，而不是 table
  * context by：类似group by，但返回的是和组内元素数量相同的向量
  * pivot by：将某列内容重新排列按照两个维度
  * asof join 操作：将左表中的每一行与右表中时间戳最接近（但不超过）的行进行连接
  * windows join：窗口连接
  * 支持窗口函数，例如mavg()窗口均值、mcorr()窗口相关系数
  * 高阶函数：参数是函数的函数
  * DDB的 group by 更灵活些，支持 by 聚合函数

* Clickhouse

  * 支持数组类型、嵌套类型
  * **不支持**pivot by操作
  * asof join
  * semi join：半匹配则连接
  * anti join：不匹配则连接
  * 窗口函数 (通过Over()子句实现)

* DuckDB

  * 支持数组类型、嵌套类型
  * timestamp 类型处理不是很方便 (不走索引的问题)
  * 创建表时**不支持**指定排序方式
  * pivot/unpivot 操作
  * asof join操作
  * semi join
  * anti join
  * lateral join
  * grouping sets：可以对多个维度执行分组 
  * 窗口函数 (通过Over()子句windows函数实现)

  ```sql
  SELECT "Plant", "Date",
      avg("MWh") OVER (
          PARTITION BY "Plant"
          ORDER BY "Date" ASC
          RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
                    AND INTERVAL 3 DAYS FOLLOWING)
          AS "MWh 7-day Moving Average"
  FROM "Generation History"
  ORDER BY 1, 2;
  ```

  > partition语法：每一个over子句（windowing）中，都隐藏了一个partition（如果没有使用partition语法），或者多个partition（如果使用了partition）。然后在每个partition内部，通过 range, preceding,following 等函数，定义一个个滑动的frame，最后，聚合运算就发生在这些frame上。

  * qualify 子句, 用于获取每个分组的某个列的前几名(或者其它条件)及它对应的整行数据




---

## 总结

​	对 Clickhouse、DDB 的表现比较满意，两者都采用了列式存储结构，支持向量化的并行计算，它们都可以处理海量数据，并提供快速的查询和分析能力。

​	其中 Clickhouse 的聚合查询速度更快些，但 DDB 也能满足需求。
DDB 支持的 SQL 算子更多，能更方便的实现一些复杂查询，大多 Clickhouse 也能够实现。
DDB 还内置 MapReduce 计算框架、流处理引擎，提供了自己的脚本语言能执行一些计算任务，Clickhouse 需要 + Pandas

​	DuckDB 不行，因为1 DuckDB是单文件的不支持分库分表、2 我测试下来性能达不到 Clickhouse、DDB 水平、3 采用索引来避免全表扫描，但是 timestamp 类型 '2020-02-10 14:00:00' 如果按年月或时分来筛速度慢 4 创建表时不支持排序键，因此插入后，每次SELECT需要排序，尽管可以通过创建排序后View视图之后再在创建的视图上查询解决每次查需要排序的问题

BTW：了解到有些私募公司没用数据库，挺多用的 [HDF5](https://www.h5py.org/) 文件系统存 tick 数据，然后用数据库的公司用 Clickhouse 的多些



未解决的问题：
在DDB在使用自定义模块的时候，发现不能加载，问了DDB社区的人也没能解决
DuckDB 有个SQL超时(没解决)，但将where条件拆开又可以，可能没走索引，全表扫描去了
