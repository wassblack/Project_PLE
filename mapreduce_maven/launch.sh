mvn package
hdfs dfs -rm -r /user/nsentout/output-project
spark-submit --num-executors 100 target/PLE--Project-0.0.1.jar
#spark-submit --executor-memory 8G --total-executor-cores 4 --num-executors 100 target/PLE--Project-0.0.1.jar
hdfs dfs -tail /user/nsentout/output-project/part-00000
#hdfs dfs -cat /user/nsentout/output-project/part-00000
#hdfs dfs -ls /user/nsentout/output-project


#mvn package
#hdfs dfs -rm -r /user/nsentout/PhasesSequenceFile
#yarn jar target/PLE--Project-0.0.1.jar /raw_data/ALCF_repo/phases.csv PhasesSequenceFile/
