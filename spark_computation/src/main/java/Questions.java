import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

public class Questions 
{
	private static Integer NUMBER_PATTERNS = 22;
	
	private static String phasesSortedInputPath = "/user/nsentout/testq7/";
	private static String patternsInputPath = "/raw_data/ALCF_repo/patterns.csv";
	
	/**********************************************************************************************/
	
	public static void question1a(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	{
		JavaRDD<Long> durationNonIdle = phasesRdd.filter(phase -> !phase.isIdle())
				.map(phase -> phase.getDuration());	
		output.add("===== DISTRIB DURATION NON IDLE =====");
		Questions.computeStats(durationNonIdle, output).fillOutput(output);
	}
	
	/**********************************************************************************************/
	
	public static void question1b(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	{
		JavaRDD<Long> durationIdle = phasesRdd.filter(phase -> phase.isIdle())
				.map(phase -> phase.getDuration());
		output.add("===== DISTRIB DURATION IDLE =====");
		Questions.computeStats(durationIdle, output).fillOutput(output);
	}
	
	/**********************************************************************************************/
	
	public static void question1c(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	{
		for (int i = 0; i < NUMBER_PATTERNS; i++) {
			String patternId = String.valueOf(i);
			JavaRDD<Long> patternDurationAlone = phasesRdd.filter(phase -> phase.getPatterns().equals(patternId))
					.map(phase -> phase.getDuration());
			output.add("DISTRIB DURATION PATTERN " + patternId + " ALONE");
			Distribution distrib = Questions.computeStats(patternDurationAlone, output);
			
			if (distrib != null)
				distrib.fillOutput(output);
		}
	}
	
	/**********************************************************************************************/
	
	public static void question2(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	{
		JavaRDD<Long> npatterns = phasesRdd.filter(phase -> !phase.isIdle())
				.map(phase -> phase.getNpatterns());
		output.add("===== DISTRIB NPATTERNS NON IDLE =====");
		Questions.computeStats(npatterns, output).fillOutput(output);
	}
	
	/**********************************************************************************************/
	
	public static void question3(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	{
		JavaRDD<Long> njobs = phasesRdd.filter(phase -> !phase.isIdle())
				.map(phase -> phase.getNjobs());
		output.add("===== DISTRIB NJOBS NON IDLE =====");
		Questions.computeStats(njobs, output).fillOutput(output);
	}
	
	/**********************************************************************************************/
	
	public static void question4(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	{
		/* a */
		// Key : job id, Value : duration
		JavaPairRDD<String, Long> jobDurationPair = phasesRdd
			.flatMapToPair(phase->  {
				List<Tuple2<String, Long>> jobsDurationTuple = new ArrayList<Tuple2<String, Long>>();
				if (!phase.isIdle()) {
					for (String job : phase.getJobs().split(",")) {
						jobsDurationTuple.add(new Tuple2<String, Long>(job, phase.getDuration()));							
					}
				}
				
				return jobsDurationTuple.iterator();
			})
			.reduceByKey((a,b) -> a+b);
		
		output.add("===== DISTRIB TOTAL ACCESS TIME PER JOB =====");
		Questions.computeStats(jobDurationPair.values(), output).fillOutput(output);
	
		/* b */
		List<Tuple2<String, Long>> topTen = jobDurationPair.top(10, new ComparatorTuple2Serializable());
		
		output.add("===== TOP TEN JOBS IN TOTAL ACCESS TIME =====");
		int cpt = 1;
		for (Tuple2<String, Long> value : topTen) {
			output.add(cpt + " : " + value._1 + " (" + value._2 + ")");
			cpt++;
		}
	}
	
	/**********************************************************************************************/
	
	public static void question5(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	{
		JavaRDD<Long> totalIdleTime = phasesRdd.filter(phase -> phase.isIdle())
				.map(phase -> phase.getDuration());
		output.add("===== TOTAL IDLE TIME =====");
		Questions.computeTotalDurationTime(totalIdleTime, output);
	}
	
	/**********************************************************************************************/
	
	public static void question6(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output, JavaSparkContext context)
	{
		/* a */
		// Key : duration, Value : pattern id
		TreeMap<Double, Long> allDurations = new TreeMap<Double, Long>(Collections.reverseOrder());
		double totalDuration = 0.0;

		// Récupère la durée où chaque pattern apparait seul ou avec d'autres patterns
		for (long i = 0; i < NUMBER_PATTERNS; i++) {
			String patternId = String.valueOf(i);
			JavaRDD<Long> patternDuration = phasesRdd.filter(phase -> phase.patternIsPresent(patternId))
					.map(phase -> phase.getDuration());

			output.add("DURATION PATTERN " + patternId + " ALONE OR WITH OTHERS PATTERNS");
			double duration = Questions.computeTotalDurationTime(patternDuration, output);
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

		/* b */
		output.add("TOP TEN PATTERNS");
		int cpt = 0;
		for (Map.Entry<Double, Long> value : allDurations.entrySet()) {
			cpt++;
			Long patternId = allDurations.get(value.getKey());
			String patternName = patternsPair.filter(pattern -> pattern._1.equals(patternId)).first()._2;
			output.add(cpt + " : " + patternName);
			if (cpt == 10)	break;
		}
	}
	
	/**********************************************************************************************/
	
	//public static void question7(JavaRDD<PhaseWritable> phasesRdd, ArrayList<String> output)
	public static void question7(String patternsInput, JavaSparkContext context)
	{
		patternsInput = patternsInput.replace(',', 'p');
		String[] patternsInputSplit = patternsInput.split("p");
		
		Arrays.sort(patternsInputSplit, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return Integer.compare(Integer.valueOf(o1), Integer.valueOf(o2));
			}
		});
		
		String outputFileName = "";
		for (int i = 0; i < patternsInputSplit.length; i++) {
			outputFileName += patternsInputSplit[i];
			if (i < patternsInputSplit.length - 1) {
				outputFileName += "p";
			}
		}
		
		outputFileName += "-r-00000";
		
		JavaRDD<String> phasesContainingInputPatterns = context.textFile(phasesSortedInputPath + outputFileName);
		phasesContainingInputPatterns.saveAsTextFile("hdfs://froment:9000/user/nsentout/output-project");
		
		/*
		String patternsInput[] = {"2","8","0","5"};
		
		output.add("count : " + phasesRdd.count());
		
		JavaRDD<PhaseWritable> phasesFilteredRdd = phasesRdd.filter(phase -> !phase.isIdle() && phase.getPatterns().split(",").length >= 4);
		
		output.add("count filtered: " + phasesFilteredRdd.count());

		JavaPairRDD<Long, HashSet<String> > test = phasesRdd
				.filter(phase -> phase.onePatternIsPresent(patternsInput))
				.flatMapToPair(phase -> {
					HashSet<Tuple2<Long, HashSet<String>>> patternPlages = new HashSet<Tuple2<Long, HashSet<String>>>();

					if (phase.onePatternIsPresent(patternsInput)) {
						String patterns = phase.getPatterns();
						List<Long> plages = phase.getPlagesHoraires();
						
						HashSet<String> value = new HashSet<>(Arrays.asList(patterns.split(",")));
						
						for (Long plage : plages) {
							patternPlages.add(new Tuple2<Long, HashSet<String>>(plage, value));
						}
					}

					return patternPlages.iterator();
				});
		
		output.add("test counter : " + test.count());
		
		
		test = test.reduceByKey((a,b) -> {
			a.addAll(b);
			return a;
		});
		
		output.add("test counter reduce: " + test.count());

		output.add("contenu rdd : ");
		for (Tuple2<Long, HashSet<String>> t : test.collect()) {
			output.add(t._1 + " :\t" + t._2);
		}
		
		ArrayList<Integer> plagesContainingInputPatterns = new ArrayList<Integer> ();
		int patternsCpt[] = new int[24];
		
		for (Tuple2<Long, HashSet<String>> pattern : test.collect()) {
			for (String p : patternsInput) {
				// si cette plage contient le pattern, on incrémente
				if (pattern._2.contains(p)) {
					int plage = Math.toIntExact(pattern._1);
					patternsCpt[plage] ++;
					
					// si elle contient les 4, c'est bon
					if (patternsCpt[plage] == 4) {
						plagesContainingInputPatterns.add(plage);
					}
				}
			}
		}
		
		output.add("Les plages choisies contiennent ces 4 patterns : ");
		plagesContainingInputPatterns.sort(Comparator.naturalOrder());
		for (Integer plage : plagesContainingInputPatterns) {
			output.add(String.valueOf(plage));
		}
		*/

	}
	
	/**********************************************************************************************/
	/**********************************************************************************************/
	
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
			distribution.setHistogram(rddDouble.histogram(10));
			
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
	
	/**********************************************************************************************/
	
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
}
