docker cp dolphindb:/data/trades_small.csv /d/qq109/Desktop/trades_small.csv
python ./时序数据库调研/csv_transfer.py /d/qq109/Desktop/trades_small.csv > /d/qq109/Desktop/trades_small_ch.csv
docker cp /d/qq109/Desktop/trades_small_ch.csv clickhouse-server:/data/trades_small_ch.csv


# cat /data/trades_big.csv | ./clickhouse-client --host=<数据库连接地址> --port=<TCP端口号> --user=<数据库账号> --password=<数据库账号的密码> --query="INSERT INTO <ClickHouse表名> FORMAT <本地文件格式>";

# clickhouse-client --query="insert into db_chtest.tb_chtest FORMAT CSVWithNames" <  /data/trades_small_ch.csv
clickhouse-client --query="insert into db_chtest.tb_chtest FORMAT CSV" <  /data/trades_small_ch.csv
# 40s