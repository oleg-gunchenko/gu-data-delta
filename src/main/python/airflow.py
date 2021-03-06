from airflow import DAG
from datetime import timedelta, datetime, now
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

dag = DAG('naumen', start_date=datetime(2019, 6, 25),
          schedule_interval='0 1 * * *',
          dagrun_timeout=timedelta(minutes=60),
          tags=['naumen', 'test'])

start = DummyOperator(dag=dag, task_id='start')


sqoop_command = """HADOOP_USER_NAME=hdfs /bin/sqoop import --connect '{{params.connection_url}}' --username '{{params.username}}'  \
  --table '{{params.table}}' --target-dir '{{params.target_dir}}' \
  --as-avrodatafile --compression-codec deflate -password {{params.password}} """

spark_command = """HADOOP_USER_NAME=hdfs /bin/spark2-submit --master local \
    --driver-memory 5G --executor-memory 6G --num-executors 1 --executor-cores 1 \
    --conf spark.executor.extraJavaOptions="-Dconfig.resource=myapp.conf -agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=0.0.0.0:7777" \
    --conf spark.driver.extraJavaOptions="-Dconfig.resource=myapp.conf -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777" \
    --class {{params.spark_class}} {{params.spark_jar}} \
    --source-path {{params.source_path}} \
    --hash-class {{params.hash_class}} --keys {{params.pkey}} --exclusions {{params.exclusions}} \
    --table {{params.table}} --table-type {{params.table_type}} --partition-key {{params.partition_key}} """

delete_target_dir_command = """
HADOOP_USER_NAME=hdfs /bin/hdfs dfs -rm -r -skipTrash {{params.path}}
echo test
"""

# tables = ['tbl_servicecall', 'tbl_objectbase', 'tbl_employee', 'tbl_ou', 'tbl_service', 'tbl_team', 'tbl_location', 'tbl_catalogs', 'tbl_servicetime', 'tbl_impact', 'tbl_urgency', 'tbl_priority', 'tbl_wayadressing', 'tbl_timezone', 'tbl_mark', 'tbl_stdonechildsc']
tables = ['tbl_ou', 'tbl_service', 'tbl_team', 'tbl_servicetime', 'tbl_impact', 'tbl_urgency', 'tbl_priority',
          'tbl_wayadressing', 'tbl_timezone', 'tbl_mark', 'tbl_stdonechildsc']
graph = []

for target_table in ['tbl_timezone']:
    target_dir = "/data/RAW/naumen/%s/export_date='%s'" % (target_table, datetime.now().strftime('%d-%m-%Y'))

    sqoop_params = {
        'target_dir': target_dir,
        'connection_url': 'jdbc:postgresql://10.5.68.232:5432/nausd40',
        'username': 'integration_eiap',
        'table': target_table,
        'password': 'pS9xkrh38M'
    }

    delta_params = {
        'spark_class': 'ru.dwh.naumen.NaumenETL',
        'spark_jar': '/home/admin7-1/naumen/delta-post-1.0-SNAPSHOT.jar',
        'hash_class': 'md5',
        'pkey': 'ID',
        'exclusions': 'created,updated',
        'source_path': target_dir,
        'table': 'etl.%s' % target_table,
        'table_type': 'dim',
        'partition_key': 'DWSJOBPART',
    }

    localDAG = dag

#    localDAG = DAG('naumen.process_%s' % target_table,
#                   schedule_interval='@once',
#                   start_date=days_ago(2),
#                   dagrun_timeout=timedelta(minutes=60),
#                   tags=['naumen', target_table])

    delete_target_dir = BashOperator(dag=localDAG, task_id='delete_target_dir_%s' % target_table,
                                     bash_command=delete_target_dir_command, params={'path': target_dir})

    import_data = BashOperator(dag=localDAG, task_id='import_data_%s' % target_table, depends_on_past=True,
                               bash_command=sqoop_command, params=sqoop_params)

    extract_delta = BashOperator(dag=localDAG, task_id='extract_delta_%s' % target_table, depends_on_past=True,
                                 bash_command=spark_command, params=delta_params)

    start >> delete_target_dir >> import_data >> extract_delta
    #graph.append(SubDagOperator(dag=dag, subdag=localDAG, task_id='process_%s' % target_table))
##    graph.append(delete_target_dir >> import_data >> extract_delta)



#start >> graph[0]
