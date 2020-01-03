mvn package
hdfs dfs -rm -r /user/nsentout/output_projet-mapreduce/
#yarn jar target/tp4-histogramme-0.0.1.jar /raw_data/worldcitiespop.txt output_histogram 10
time yarn jar target/projet-mapreduce-distribution-0.0.1.jar /raw_data/ALCF_repo/phases.csv output_projet-mapreduce 10
#time yarn jar target/projet-mapreduce-distribution-0.0.1.jar /user/nsentout/cropped-phases.csv output_projet-mapreduce 10
hdfs dfs -cat /user/nsentout/output_projet-mapreduce/part-r-00000
