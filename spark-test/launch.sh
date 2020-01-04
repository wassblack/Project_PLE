mvn package
#spark-submit --class bigdata.SparkTest --executor-memory 8G --total-executor-cores 4 target/projet-spark_test-0.0.1.jar
spark-submit --num-executors 100 target/projet-spark_test-0.0.1.jar

