
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
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
	
	private static String inputPath = "/user/nsentout/PhasesSequenceFile/part-m-002*";
	//private static String inputPath = "/user/nsentout/PhasesSequenceFile/*";
	
	private static Integer NUMBER_PHASES = 22;

	public static void main(String[] args)
	{	
		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		ArrayList<String> output = new ArrayList<String>();

		JavaRDD<PhaseWritable> phasesRdd = context.sequenceFile(inputPath, NullWritable.class, PhaseWritable.class)
				.values().persist(StorageLevel.MEMORY_AND_DISK());
		
		// Question 1 a
		/*
		JavaRDD<Long> durationNonIdle = phasesRdd.filter(f -> !f.getPatterns().equals("-1"))
					.map(f -> f.getDuration());	
		output.add("===== DISTRIB DURATION NON IDLE =====");
		SparkTest.computeStats(durationNonIdle, output);
		*/
		
		// Question 1 b
		/*
		JavaRDD<Long> durationIdle = phasesRdd.filter(f -> f.getPatterns().equals("-1"))
				.map(f -> f.getDuration());
		output.add("===== DISTRIB DURATION IDLE =====");
		SparkTest.computeStats(durationIdle, output);
		*/
		
		// Question 1 c
		/*
		for (int i = 0; i < NUMBER_PHASES; i++) {
			final int k = i;
			JavaRDD<Long> phases = phasesRdd.filter(f -> f.getPatterns().equals(String.valueOf(k)))
					.map(f -> f.getDuration());
			output.add("DISTRIB DURATION PHASE " + k + " ALONE");
			SparkTest.computeStats(phases, output);
		}
		*/
		
		/*
		// Question 2
		JavaRDD<Long> npatterns = phasesRdd.filter(f -> !f.getPatterns().equals("-1"))
				.map(f -> (long)f.getNpatterns());
		output.add("===== DISTRIB NPATTERNS NON IDLE =====");
		SparkTest.computeStats(npatterns, output);
		*/
		
		/*
		// Question 3
		JavaRDD<Long> njobs = phasesRdd.filter(f -> !f.getPatterns().equals("-1"))
				.map(f -> (long)f.getNjobs());
		output.add("===== DISTRIB NJOBS NON IDLE =====");
		SparkTest.computeStats(njobs, output);
		*/
		
		// Question 5
		/*
		JavaRDD<Long> totalIdleTime = phasesRdd.filter(f -> f.getPatterns().equals("-1"))
				.map(f -> f.getDuration());
		output.add("===== TOTAL IDLE TIME =====");
		SparkTest.computeTotalDurationTime(totalIdleTime, output);
		*/
		
		// Question 6
		
		TreeMap<Double, String> allDurations = new TreeMap<Double, String>(Collections.reverseOrder());
		double totalDuration = 0.0;
				
		for (int i = 0; i < NUMBER_PHASES; i++) {
			final int k = i;
			JavaRDD<Long> phaseAlone = phasesRdd.filter(f -> f.getPatterns().equals(String.valueOf(k)))
					.map(f -> f.getDuration());
			output.add("DURATION PHASE " + k + " ALONE");
			double duration = SparkTest.computeTotalDurationTime(phaseAlone, output);
			if (duration != -1.0) {
				allDurations.put(duration, String.valueOf(i));
				totalDuration += duration;
			}
		}
		
		output.add("TOTAL DURATION " + totalDuration);

		for (Map.Entry<Double, String> value : allDurations.entrySet()) {
			output.add("PERCENTAGE PHASE " + value.getValue() + " ALONE");
			double percentage = value.getKey() / totalDuration * 100;
			output.add(String.valueOf(percentage));
		}
		
		output.add("TOP TEN PATTERNS");
		int cpt = 0;
		for (Map.Entry<Double, String> value : allDurations.entrySet()) {
			cpt++;
			output.add(cpt + " : " + allDurations.get(value.getKey()));
			if (cpt == 9)	break;
		}
		

		context.parallelize(output).repartition(1).saveAsTextFile("hdfs://froment:9000/user/nsentout/output-project");
	}
	
	private static double computeTotalDurationTime(JavaRDD<Long> rdd, ArrayList<String> output)
	{
		JavaDoubleRDD rddDouble = rdd.mapToDouble(f -> f);
		long count = rddDouble.count();
		
		if (count > 0) {
			StatCounter statistics = rddDouble.stats();
			double hours = statistics.sum() / 1000.0 / 3600.0;
			output.add("Duration (in hours) : " + hours);
			return hours;
		}
		else {
			output.add("The RDD was empty");
		}
		return -1.0;
	}
	
	private static void computeStats(JavaRDD<Long> rdd, ArrayList<String> output)
	{
		JavaDoubleRDD rddDouble = rdd.mapToDouble(f -> f);
		long count = rddDouble.count();
		
		if (count > 0) {
			StatCounter statistics = rddDouble.stats();
			
			output.add("min : " + statistics.min());
			output.add("max : " + statistics.max());
			output.add("avg : " + statistics.mean());
			output.add("total : " + statistics.sum());
		
			// Histogramme
			output.add("Histogramme : ");
			Tuple2<double[], long[]> histogram = rddDouble.histogram(10);
	
			for (int i = 0; i < histogram._1.length - 1; i++) {
				StringBuilder histogramString = new StringBuilder();
	
				histogramString.append("[" + histogram._1[i] + ", " + histogram._1[i+1]);
				if (i == histogram._1.length - 2) {
					histogramString.append("] : ");
				}
				else {
					histogramString.append(") : ");
				}
				histogramString.append(histogram._2[i]);
	
				output.add(histogramString.toString());
			}
			
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
				output.add("median : " + median);
				
				long first_quarter_left = count / 4 - 1;
				long first_quarter_right = first_quarter_left + 1;
				firstQuartile = (rddPair.lookup(first_quarter_left).get(0) + rddPair.lookup(first_quarter_right).get(0)) / 2;
				output.add("premier quartile : " + firstQuartile);
				
				long third_quarter_left = 3 * (count / 4) - 1;
				long third_quarter_right = third_quarter_left + 1;
				thirdQuartile = (rddPair.lookup(third_quarter_left).get(0) + rddPair.lookup(third_quarter_right).get(0)) / 2;
				output.add("troisieme quartile : " + thirdQuartile);
			}
			else {
				median = rddPair.lookup(count / 2).get(0);
				output.add("median : " + median);
				firstQuartile = rddPair.lookup(count / 4).get(0);
				output.add("premier quartile : " + firstQuartile);
				thirdQuartile = rddPair.lookup(3 * count / 4).get(0);
				output.add("troisieme quartile : " + thirdQuartile);
				
			}	
		}
		else {
			output.add("The RDD was empty");
		}
	}
	
}

