#!/usr/bin/env bash

# screen -dmS sqoop-$1 sh -c "sqoop import --connect 'jdbc:postgresql://10.5.68.232:5432/nausd40' --username 'integration_eiap'  --table '$1' --target-dir 'naumen/$1' --as-avrodatafile --compression-codec deflate -password pS9xkrh38M"

# https://stackoverflow.com/questions/37160471/how-to-store-password-in-password-file-sqoop


screen -dmS sqoop-tbl_servicecall sh -c "sqoop import --connect 'jdbc:postgresql://10.5.68.232:5432/nausd40' --username 'integration_eiap'  --table 'tbl_servicecall' --target-dir '/data/RAW/naumen/tbl_servicecall/export_date=$(date '+%Y-%m-%d')' --as-avrodatafile --compression-codec deflate -password pS9xkrh38M"
screen -dmS sqoop-tbl_objectbase sh -c "sqoop import --connect 'jdbc:postgresql://10.5.68.232:5432/nausd40' --username 'integration_eiap'  --table 'tbl_objectbase' --target-dir '/data/RAW/naumen/naumen/tbl_objectbase/export_date=$(date '+%Y-%m-%d')' --as-avrodatafile --compression-codec deflate -password pS9xkrh38M"

for x in $(cat tables.lst); do kite-dataset create dataset:hdfs:naumen/$x --schema sqoop/$x.avsc; done;

export HADOOP_USER_NAME=hdfs
hdfs dfs -rm -r -skipTrash /data/RAW/naumen
for x in $(cat tables.lst); do hdfs dfs -mkdir -p /data/RAW/naumen/$x/export_date=$(date '+%Y-%m-%d'); done

for x in $(cat tables.lst); do hdfs dfs -cp /user/niips0/naumen/$x/* /data/RAW/naumen/$x/export_date=$(date '+%Y-%m-%d'); done

for x in $(cat tables.lst); do hdfs dfs -cp -r /user/niips0/naumen/$x/.metadata /data/RAW/naumen/$x; done


for x in $(cat short_tables.lst); do
screen -dmS sqoop-$x sh -c "sqoop import --connect 'jdbc:postgresql://10.5.68.232:5432/nausd40' --username 'integration_eiap'  --table '$x' --target-dir '/data/RAW/naumen/$x/export_date=$(date '+%Y-%m-%d')' --as-avrodatafile --compression-codec deflate -password pS9xkrh38M";
done;