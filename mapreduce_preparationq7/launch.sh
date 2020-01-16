mvn package
hdfs dfs -rm -r /user/nsentout/q7prep
yarn jar target/mapreduce-q7preparation-0.0.1.jar /user/nsentout/cropped-phases.csv /user/nsentout/q7prep
hdfs dfs -ls /user/nsentout/q7prep/