CREATE EXTERNAL TABLE analytics.st_cloud9kinesisathena
(
`USER_ID` string,
`INITIAL_EVENT` string,
`EVENT_2` string,
`EVENT_3` string,
`EVENT_OUT` string,
`OS_USER` string,
`CITY` string,
`LATITUD` double,
`LONGITUD` double,
`ORDER_TYPE` string,
`STATUS` string,
`PAYMENT_METHOD` string,
`CREATED_AT`string
)
PARTITIONED BY ( `year` string, `month` string, `day` string, `hour` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://gt-datalake-dev-us-east-2-508186271604-stage/de/st_Cloud9KinesisAthena/'