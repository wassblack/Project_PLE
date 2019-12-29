mvn package
spark-submit --class bigdata.NjobsDistribution --executor-memory 8G --total-executor-cores 4 target/projet-njobs-distrib-0.0.1.jar