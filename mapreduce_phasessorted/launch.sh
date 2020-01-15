mvn package
hdfs dfs -rm -r /user/nsentout/testq7
yarn jar target/mapreduce-phasesorted-0.0.1.jar /user/nsentout/cropped-phases.csv /user/nsentout/testq7
hdfs dfs -ls /user/nsentout/testq7/