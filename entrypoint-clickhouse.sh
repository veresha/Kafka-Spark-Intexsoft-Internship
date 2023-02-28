#! /bin/sh

clickhouse-client -q "CREATE DATABASE IF NOT EXISTS quotes-db"

clickhouse-client -q "CREATE TABLE IF NOT EXISTS quotes-db.quotes(
  company String,
  period_start DateTime,
  records_count UInt32,
  avg Float32,
  max Float32,
  min Float32,
  ) ENGINE = MergeTree()
  ORDER BY (company, period_start)
  PRIMARY KEY (company, period_start)"