
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.TreeMap;
import java.lang.Long;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;


public class SparkTest {
	
	//private static String phasesInputPath = "/user/nsentout/PhasesSequenceFile/part-m-002*";
	private static String phasesInputPath = "/user/nsentout/PhasesSequenceFile/*";
	
	private static String patternsInputPath = "/raw_data/ALCF_repo/patterns.csv";
	
	private static Integer NUMBER_PATTERNS = 22;
	private static Integer NUMBER_JOBS = 85421;

	public static void main(String[] args)
	{	
		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		ArrayList<String> output = new ArrayList<String>();

		JavaRDD<PhaseWritable> phasesRdd = context.sequenceFile(phasesInputPath, NullWritable.class, PhaseWritable.class)
				.values().persist(StorageLevel.MEMORY_AND_DISK());
		
		// Question 1 a
		/*
		JavaRDD<Long> durationNonIdle = phasesRdd.filter(phase -> !phase.getPatterns().equals("-1"))
					.map(phase -> phase.getDuration());	
		output.add("===== DISTRIB DURATION NON IDLE =====");
		SparkTest.computeStats(durationNonIdle, output).fillOutput(output);
		*/
		
		// Question 1 b
		/*
		JavaRDD<Long> durationIdle = phasesRdd.filter(phase -> phase.getPatterns().equals("-1"))
				.map(phase -> phase.getDuration());
		output.add("===== DISTRIB DURATION IDLE =====");
		SparkTest.computeStats(durationIdle, output);
		*/
		
		// Question 1 c
		/*
		for (int i = 0; i < NUMBER_PATTERNS; i++) {
			String patternId = String.valueOf(i);
			JavaRDD<Long> patternDurationAlone = phasesRdd.filter(phase -> phase.getPatterns().equals(patternId))
					.map(phase -> phase.getDuration());
			output.add("DISTRIB DURATION PATTERN " + patternId + " ALONE");
			Distribution distrib = SparkTest.computeStats(patternDurationAlone, output);
			
			if (distrib != null)
				distrib.fillOutput(output);
		}
		*/
		
		// Question 2
		/*
		JavaRDD<Long> npatterns = phasesRdd.filter(phase -> !phase.getPatterns().equals("-1"))
				.map(phase -> phase.getNpatterns());
		output.add("===== DISTRIB NPATTERNS NON IDLE =====");
		SparkTest.computeStats(npatterns, output).fillOutput(output);
		*/
		
		// Question 3
		/*
		JavaRDD<Long> njobs = phasesRdd.filter(phase -> !phase.getPatterns().equals("-1"))
				.map(phase -> phase.getNjobs());
		output.add("===== DISTRIB NJOBS NON IDLE =====");
		SparkTest.computeStats(njobs, output).fillOutput(output);
		*/
		
		// Question 4 a (en cours)
		/*
		JavaPairRDD<String, Long> accessTimeJob = phasesRdd.filter(phase -> !phase.getPatterns().equals("-1"))
				.mapToPair(phase -> {
					String jobs = phase.getJobs();
					Long accessTime = phase.getDuration();

					return new Tuple2<String, Long>(jobs, accessTime);
				});
		*/
		/*
		List<Double> totalAccessTimeJobs = new ArrayList<Double>();
		TreeMap<Double, Long> topTenJob = new TreeMap<Double, Long>(Collections.reverseOrder());
		
		for (long i = 0; i < NUMBER_JOBS; i++) {
			final long k = i;
			JavaRDD<Long> jobAccessTime = phasesRdd.filter(phase -> phase.getJobs().contains(String.valueOf(k)))
					.map(phase -> phase.getDuration());
			output.add("===== DISTRIB ACCESS TIME JOB " + k + " =====");
			double totalAccessTime = SparkTest.computeTotalDurationTime(jobAccessTime, output);
			totalAccessTimeJobs.add(totalAccessTime);
			
			
			//topTenJob.put(distribution.getSum(), k);
			//if(topTenJob.size() > 10)
				topTenJob.remove(topTenJob.firstKey());
		}
		
		// Question 4 b (en cours)
		output.add("TOP TEN JOBS ACCESS TIME");
		int cpt = 0;
		for (Map.Entry<Double, Long> value : topTenJob.entrySet()) {
			cpt++;
			Long jobId = value.getValue();
			output.add(cpt + " : " + jobId);
		}
		*/

		/*
		HashMap<String, Long> accessPerJob = new HashMap<String, Long>();
		
		JavaRDD<PhaseWritable> accessTimeJob = phasesRdd.filter(phase -> !phase.getPatterns().equals("-1"));
		
		accessTimeJob.foreach(phase -> {
			String jobs = phase.getJobs();
			Long duration = phase.getDuration();
			
			for (String job : jobs.split(",")) {
				if (accessPerJob.containsKey(job)) {
					accessPerJob.put(job, accessPerJob.get(job) + duration);
				}
				else {
					accessPerJob.put(job, duration);	
				}
			}
		});
		*/

		
		// Question 5
		/*
		JavaRDD<Long> totalIdleTime = phasesRdd.filter(phase -> phase.getPatterns().equals("-1"))
				.map(phase -> phase.getDuration());
		output.add("===== TOTAL IDLE TIME =====");
		SparkTest.computeTotalDurationTime(totalIdleTime, output);
		*/
		
		// Question 6 a
		// Key : duration, Value : pattern id
		TreeMap<Double, Long> allDurations = new TreeMap<Double, Long>(Collections.reverseOrder());
		double totalDuration = 0;
				
		// Récupère la durée où chaque pattern apparait seul ou avec d'autres patterns
		for (long i = 0; i < NUMBER_PATTERNS; i++) {
			String patternId = String.valueOf(i);
			JavaRDD<Long> patternDuration = phasesRdd.filter(phase -> phase.patternIsPresent(patternId))
					.map(phase -> phase.getDuration());
			
			output.add("DURATION PATTERN " + patternId + " ALONE OR WITH OTHERS PATTERNS");
			double duration = SparkTest.computeTotalDurationTime(patternDuration, output);
			if (duration != -1.0) {
				allDurations.put(duration, i);
				totalDuration += duration;
			}
		}
		
		output.add("TOTAL DURATION " + totalDuration);
		
		// Lit le fichier patterns.csv pour récupérer le nom du pattern à partir de son id
		JavaRDD<String> patterns = context.textFile(patternsInputPath, 4).filter(pattern -> !pattern.equals("id;name"));
		JavaPairRDD<Long, String> patternsPair = patterns.mapToPair(pattern -> {
			String[] patternSplits = pattern.split(";");
			Long id = Long.valueOf(patternSplits[0]);
			String name = patternSplits[1];

			return new Tuple2<Long, String>(id, name);
		});

		// Récupère les pourcentages de chaque pattern seul ou avec d'autres patterns
		for (Map.Entry<Double, Long> value : allDurations.entrySet()) {
			Long patternId = allDurations.get(value.getKey());
			String patternName = patternsPair.filter(pattern -> pattern._1.equals(patternId)).first()._2;
			output.add("PERCENTAGE PATTERN \"" + patternName + "\" ALONE OR WITH OTHERS PATTERNS");
			double percentage = value.getKey() / totalDuration * 100;
			output.add(String.valueOf(percentage));
		}
		
		// Question 6 b
		output.add("TOP TEN PATTERNS");
		int cpt = 0;
		for (Map.Entry<Double, Long> value : allDurations.entrySet()) {
			cpt++;
			Long patternId = allDurations.get(value.getKey());
			String patternName = patternsPair.filter(pattern -> pattern._1.equals(patternId)).first()._2;
			output.add(cpt + " : " + patternName);
			if (cpt == 10)	break;
		}
		

		context.parallelize(output).repartition(1).saveAsTextFile("hdfs://froment:9000/user/nsentout/output-project");
	}
	
	private static double computeTotalDurationTime(JavaRDD<Long> rdd, ArrayList<String> output)
	{
		long count = rdd.count();
		
		if (count > 0) {
			long totalDuration = rdd.reduce((a,b)->a+b);
			double hours = totalDuration / 1000000.0 / 3600.0;
			output.add("Duration (in hours) : " + hours);
			output.add("Duration (in days) : " + hours / 24.0);
			return hours;
		}
		else {
			output.add("The RDD was empty");
		}
		return -1.0;
	}
	
	private static void computeStats2(JavaRDD<Long> rdd, ArrayList<String> output)
	{
		JavaDoubleRDD rddDouble = rdd.mapToDouble(f -> f);
		long count = rddDouble.count();
		System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1");
		
		if (count > 0) {
			StatCounter statistics = rddDouble.stats();

			output.add("Min : " + statistics.min());
			output.add("Max : " + statistics.max());
			output.add("Avg : " + statistics.mean());
			output.add("Total : " + statistics.sum());
			
			System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA2");
		
			// Histogramme
			/*
			output.add("Histogram : ");
			Tuple2<double[], long[]> histogram = rddDouble.histogram(10);
			
			System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA2.01");
			StringBuilder histogramString = new StringBuilder();
			
			System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA2.1 (" + histogram._1.length + ")");
				
			for (int i = 0; i < histogram._1.length - 1; i++) {
				
	
				histogramString.append("[" + histogram._1[i] + ", " + histogram._1[i+1]);
				if (i == histogram._1.length - 2) {
					histogramString.append("] : ");
				}
				else {
					histogramString.append(") : ");
				}
				histogramString.append(histogram._2[i]);
	
				
			}
			System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA2.5");
			output.add(histogramString.toString());
			System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA3");
			*/
			// Récupère la médiane et les quartiles
			JavaPairRDD<Long, Long> rddPair = rdd.sortBy(f -> f, true, rdd.getNumPartitions())
						.zipWithIndex().mapToPair(f -> new Tuple2<>(f._2, f._1));
			
			//JavaPairRDD<Long, Long> rddPair = rdd.zipWithIndex().mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey();
			long median = 0;
			long firstQuartile = 0;
			long thirdQuartile = 0;
			
			if (count % 2 == 0) {
				long middle_left = count / 2 - 1;
				long middle_right = middle_left + 1;
				median = (rddPair.lookup(middle_left).get(0) + rddPair.lookup(middle_right).get(0)) / 2;
				output.add("Median : " + median);
				
				long first_quarter_left = count / 4 - 1;
				long first_quarter_right = first_quarter_left + 1;
				firstQuartile = (rddPair.lookup(first_quarter_left).get(0) + rddPair.lookup(first_quarter_right).get(0)) / 2;
				output.add("First quartile : " + firstQuartile);
				
				long third_quarter_left = 3 * (count / 4) - 1;
				long third_quarter_right = third_quarter_left + 1;
				thirdQuartile = (rddPair.lookup(third_quarter_left).get(0) + rddPair.lookup(third_quarter_right).get(0)) / 2;
				output.add("Third quartile : " + thirdQuartile);
			}
			else {
				median = rddPair.lookup(count / 2).get(0);
				output.add("Median : " + median);
				
				firstQuartile = rddPair.lookup(count / 4).get(0);
				output.add("First quartile : " + firstQuartile);
				
				thirdQuartile = rddPair.lookup(3 * count / 4).get(0);
				output.add("Third quartile : " + thirdQuartile);
				
			}
			
		}
		else {
			output.add("The RDD was empty");
		}
	}
	
	private static Distribution computeStats(JavaRDD<Long> rdd, ArrayList<String> output)
	{
		JavaDoubleRDD rddDouble = rdd.mapToDouble(f -> f);
		long count = rddDouble.count();
		
		if (count > 0) {
			Distribution distribution = new Distribution();
			StatCounter statistics = rddDouble.stats();
			
			distribution.setMin(statistics.min());
			distribution.setMax(statistics.max());
			distribution.setAvg(statistics.mean());
		
			// Histogramme
			//distribution.setHistogram(rddDouble.histogram(10));
			
			// Récupère la médiane et les quartiles
			JavaPairRDD<Long, Long> rddPair = rdd.sortBy(f -> f, true, rdd.getNumPartitions())
						.zipWithIndex().mapToPair(f -> new Tuple2<>(f._2, f._1));
			
			//JavaPairRDD<Long, Long> rddPair = rdd.zipWithIndex().mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey();
			
			long median = 0;
			long firstQuartile = 0;
			long thirdQuartile = 0;
			
			if (count % 2 == 0) {
				long middle_left = count / 2 - 1;
				long middle_right = middle_left + 1;
				median = (rddPair.lookup(middle_left).get(0) + rddPair.lookup(middle_right).get(0)) / 2;
				
				long first_quarter_left = count / 4 - 1;
				long first_quarter_right = first_quarter_left + 1;
				firstQuartile = (rddPair.lookup(first_quarter_left).get(0) + rddPair.lookup(first_quarter_right).get(0)) / 2;
				
				long third_quarter_left = 3 * (count / 4) - 1;
				long third_quarter_right = third_quarter_left + 1;
				thirdQuartile = (rddPair.lookup(third_quarter_left).get(0) + rddPair.lookup(third_quarter_right).get(0)) / 2;
			}
			else {
				median = rddPair.lookup(count / 2).get(0);
				firstQuartile = rddPair.lookup(count / 4).get(0);
				thirdQuartile = rddPair.lookup(3 * count / 4).get(0);
			}
			
			distribution.setMedian(median);
			distribution.setFirstQuartile(firstQuartile);
			distribution.setThirdQuartile(thirdQuartile);
			
			return distribution;
		}
		else {
			output.add("The RDD was empty");
		}
		
		return null;
	}
}

