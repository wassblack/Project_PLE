mvn package
hdfs dfs -rm -r /user/nsentout/output-project
spark-submit --num-executors 100 target/PLE--Project-0.0.1.jar
hdfs dfs -ls /user/nsentout/output-project



#-rw-r--r--   3 nsentout supergroup          0 2020-01-08 15:47 /user/nsentout/output-project/_SUCCESS
#-rw-r--r--   3 nsentout supergroup          0 2020-01-08 15:47 /user/nsentout/output-project/part-00000
#-rw-r--r--   3 nsentout supergroup          9 2020-01-08 15:47 /user/nsentout/output-project/part-00001
#-rw-r--r--   3 nsentout supergroup          0 2020-01-08 15:47 /user/nsentout/output-project/part-00002
#-rw-r--r--   3 nsentout supergroup          9 2020-01-08 15:47 /user/nsentout/output-project/part-00003
#-rw-r--r--   3 nsentout supergroup         24 2020-01-08 15:47 /user/nsentout/output-project/part-00004
#-rw-r--r--   3 nsentout supergroup          0 2020-01-08 15:47 /user/nsentout/output-project/part-00005
#-rw-r--r--   3 nsentout supergroup         17 2020-01-08 15:47 /user/nsentout/output-project/part-00006
#-rw-r--r--   3 nsentout supergroup         10 2020-01-08 15:47 /user/nsentout/output-project/part-00007