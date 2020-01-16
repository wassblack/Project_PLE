mvn package
hdfs dfs -rm -r /user/nsentout/output-project
spark-submit --executor-memory 8G --total-executor-cores 4 --num-executors 100 target/spark-computation-0.0.1.jar $1 $2
hdfs dfs -tail /user/nsentout/output-project/part-00000
