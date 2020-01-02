mvn package
#spark-submit --class bigdata.SparkTest --executor-memory 8G --total-executor-cores 4 target/projet-spark_test-0.0.1.jar
spark-submit --class bigdata.SparkTest --master yarn --deploy-mode cluster --executor-memory 7G --total-executor-cores 4 --num-executors 100 target/projet-spark_test-0.0.1.jar
#spark-submit --class bigdata.SparkTest --master yarn --deploy-mode client --executor-memory 7G --total-executor-cores 4 --num-executors 10 target/projet-spark_test-0.0.1.jar
