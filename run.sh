#!/usr/bin/env bash

source ~/.bash_profile

#spark-submit --master local  --driver-memory 5G --executor-memory 6G --num-executors 1 --executor-cores 1 \
#--conf spark.executor.extraJavaOptions="-Dconfig.resource=myapp.conf -agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=0.0.0.0:7777" \
#--conf spark.driver.extraJavaOptions="-Dconfig.resource=myapp.conf -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777" \
#--class ru.dwh.naumen.NaumenETL \
#target/delta-post-1.0-SNAPSHOT.jar ddl md5 --source-path data/etl-test/origin \
#  --table etl.test --table-type dim --partition-key DWSJOBID


spark2-submit --master local  --driver-memory 5G --executor-memory 6G --num-executors 1 --executor-cores 1 \
--conf spark.executor.extraJavaOptions="-Dsun.io.serialization.extendedDebugInfo=true -agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=0.0.0.0:7777" \
--conf spark.driver.extraJavaOptions="-Dsun.io.serialization.extendedDebugInfo=true -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777" \
--class ru.niips.dwh.naumen.NaumenETL \
naumen/delta-post-1.0-SNAPSHOT.jar add-partition --source-path /user/hive/warehouse/nautest.db/tbl_ds01/partition=458f57e4-a773-48d6-adab-36500e8f8bcd \
  --table nautest.tbl_ds02 --table-type dim --partition-key PARTITION --partition-value 458f57e4-a773-48d6-adab-36500e8f8bcd

#spark2-submit --master local  --driver-memory 5G --executor-memory 6G --num-executors 1 --executor-cores 1 \
#--conf spark.executor.extraJavaOptions="-Dconfig.resource=myapp.conf -agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=0.0.0.0:7777" \
#--conf spark.driver.extraJavaOptions="-Dconfig.resource=myapp.conf -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777" \
#--class ru.dwh.naumen.NaumenETL \
#naumen/delta-post-1.0-SNAPSHOT.jar add-partition --source-path /user/hive/warehouse/nautest.db/tbl_ds01/partition=458f57e4-a773-48d6-adab-36500e8f8bcd \
#  --table nautest.tbl_ds02 --partition-key PARTITION --partition-value 458f57e4-a773-48d6-adab-36500e8f8bcd
