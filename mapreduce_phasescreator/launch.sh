mvn package
hdfs dfs -rm -r /user/nsentout/PhaseSequenceFiles
yarn jar target/mapreduce-phasecreator-0.0.1.jar /raw_data/ALCF_repo/phases.csv /user/nsentout/PhaseSequenceFiles