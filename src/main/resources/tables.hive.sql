create database naumen;

hdfs dfs -mkdir -p /data/RAW/naumen/$1/export_date=$(date '+%Y-%m-%d')


CREATE EXTERNAL TABLE $1_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/$1' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/$1/.metadata/schema.avsc');


CREATE EXTERNAL TABLE tbl_servicecall_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_servicecall' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_servicecall/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_objectbase_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_objectbase' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_objectbase/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_employee_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_employee' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_employee/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_ou_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_ou' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_ou/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_service_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_service' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_service/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_team_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_team' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_team/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_location_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_location' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_location/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_catalogs_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_catalogs' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_catalogs/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_servicetime_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_servicetime' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_servicetime/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_impact_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_impact' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_impact/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_urgency_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_urgency' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_urgency/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_priority_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_priority' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_priority/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_wayadressing_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_wayadressing' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_wayadressing/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_timezone_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_timezone' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_timezone/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_mark_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_mark' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_mark/.metadata/schema.avsc');
CREATE EXTERNAL TABLE tbl_stdonechildsc_src PARTITIONED BY (export_date STRING) STORED AS AVRO LOCATION '/data/RAW/naumen/tbl_stdonechildsc' TBLPROPERTIES ('avro.schema.url' = 'hdfs:///data/RAW/naumen/tbl_stdonechildsc/.metadata/schema.avsc');


alter table $1_src add partition(export_date='2020-06-07');

alter table tbl_servicecall_src drop partition(export_date='2020-06-08');
alter table tbl_objectbase_src drop partition(export_date='2020-06-08');

alter table tbl_servicecall_src add partition(export_date='2020-06-07');
alter table tbl_objectbase_src add partition(export_date='2020-06-07');
alter table tbl_employee_src add partition(export_date='2020-06-07');
alter table tbl_ou_src add partition(export_date='2020-06-07');
alter table tbl_service_src add partition(export_date='2020-06-07');
alter table tbl_team_src add partition(export_date='2020-06-07');
alter table tbl_location_src add partition(export_date='2020-06-07');
alter table tbl_catalogs_src add partition(export_date='2020-06-07');
alter table tbl_servicetime_src add partition(export_date='2020-06-07');
alter table tbl_impact_src add partition(export_date='2020-06-07');
alter table tbl_urgency_src add partition(export_date='2020-06-07');
alter table tbl_priority_src add partition(export_date='2020-06-07');
alter table tbl_wayadressing_src add partition(export_date='2020-06-07');
alter table tbl_timezone_src add partition(export_date='2020-06-07');
alter table tbl_mark_src add partition(export_date='2020-06-07');
alter table tbl_stdonechildsc_src add partition(export_date='2020-06-07');


/*
tbl_servicecall
tbl_objectbase
tbl_employee
tbl_ou
tbl_service
tbl_team
tbl_location
tbl_catalogs
tbl_servicetime
tbl_impact
tbl_urgency
tbl_priority
tbl_wayadressing
tbl_timezone
tbl_mark
tbl_stdonechildsc
*/


insert into tbl_ds01(ID, T1, T2, T3)
 values (1, "T11", "T12", "T13"),
        (2, "T21", "T22", "T23"),
        (3, "T31", "T32", "T33"),
        (4, "T41", "T42", "T43");


insert into tbl_ds01(ID, T1, T2, T3)
values (5, "T51", "T52", "T53");


update tbl_ds01 set T1 = "T2U" where id = 2;
update tbl_ds01 set T2 = "T3U" where id = 3;


delete from tbl_ds01 where id = 2;

truncate table tbl_ds01;

insert into tbl_ds01(ID, T1, T2, T3)
values (1, "T11", "T12", "T13"),
  (2, "T21", "T22", "T23"),
  (3, "T31", "T32", "T33"),
  (4, "T41", "T42", "T43"),
  (5, "T51", "T52", "T53");

delete from tbl_ds01 where id in (1, 3);

update tbl_ds01 set T1 = "T2U" where id = 2;
update tbl_ds01 set T3 = "T3U" where id = 5;


insert into tbl_ds01(ID, T1, T2, T3)
values (6, "T61", "T62", "T63");



drop table etl.test2_src;

create table if not exists etl.test2_src (ID INT,T1 STRING,T2 STRING,T3 STRING)
  partitioned by (yy int, mn int, dy int) stored as avro
  TBLPROPERTIES (
  'avro.schema.url'='/user/niips0/data/etl-test/origin.avsc');

hdfs dfs -mkdir -p /user/hive/warehouse/etl.db/test2_src/year=2020/month=12/day=5/
hdfs dfs -cp data/etl-test/new/* /user/hive/warehouse/etl.db/test2_src/year=2020/month=12/day=5/

drop table etl.test2_nklink;
create table if not exists etl.test2_nklink (nk STRING,dwsuniact STRING,dwsarchive STRING,dwskeyhash STRING,dwsvalhash STRING,id INT,t1 STRING,yy INT,mn INT,dy INT)
partitioned by (dwsjob string)
stored as orc;


create or replace view etl.test2 as
select *
from etl.test2_src s
     join etl.test2_nklink n on s.id = n.id and s.yy = n.yy and s.mn = n.mn and s.t1 = n.t1 and s.dy = n.dy;

