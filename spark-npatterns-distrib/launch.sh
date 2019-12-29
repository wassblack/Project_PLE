mvn package
#spark-submit --class bigdata.NpatternsDistribution --executor-memory 512M --total-executor-cores 2 target/projet-npatterns-distrib-0.0.1.jar
spark-submit --class bigdata.NpatternsDistribution --executor-memory 8G --total-executor-cores 4 target/projet-npatterns-distrib-0.0.1.jar
#spark-submit --class bigdata.NpatternsDistribution --master yarn --deploy-mode cluster --executor-memory 7G --total-executor-cores 4 --num-executors 10 target/projet-npatterns-distrib-0.0.1.jar
#spark-submit --class bigdata.NpatternsDistribution --master yarn --deploy-mode client --executor-memory 7G --total-executor-cores 4 --num-executors 10 target/projet-npatterns-distrib-0.0.1.jar
