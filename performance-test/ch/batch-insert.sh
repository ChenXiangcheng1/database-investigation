#!/bin/bash

for i in {0..64}
do
  echo ${i}
  clickhouse-client --query="insert into db_snapshot_SH_L2_100G.tb_snapshot_SH_L2_MergeTree FORMAT CSV" <  /data/test_${i}.csv
done
