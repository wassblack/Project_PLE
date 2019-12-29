package bigdata;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

public class NjobsDistribution {
	
	private static String njobsColumnName = "njobs";
	private static String inputPath = "/raw_data/ALCF_repo/phases.csv";
	//private static String inputPath = "/user/nsentout/cropped-phases.csv";

	public static void main(String[] args)
	{	
		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		// Lit le fichier de données
		JavaRDD<String> rdd = context.textFile(inputPath, 4);	// 4 est le nombre de partitions

		// Parse le fichier csv pour récupérer seulement la colonne njobs et le status de la phase (idle ou non)
		JavaPairRDD<String, Boolean> rdd_tuple = rdd.mapToPair(line -> {
			String[] lineSplits = line.split(";");
			boolean isIdle = lineSplits[3].equals("-1");
			String njobs = lineSplits[6];

			return new Tuple2<String, Boolean>(njobs, isIdle);
		});
		
		// Filtre pour enlever les phases idle et la première ligne décrivant les colonnes
		Function<Tuple2<String, Boolean>, Boolean> filterFunction = item -> (item._2 == false && !item._1.isEmpty() && !item._1.equals(njobsColumnName));
		rdd_tuple = rdd_tuple.filter(filterFunction);
		
		// Trie les nphases par ordre croissant
		rdd_tuple = rdd_tuple.sortByKey();
		
		// Convertit le PairRDD en DoubleRDD pour récupérer les stats
		JavaDoubleRDD rdd_npatterns = rdd_tuple.mapToDouble(item -> new Double(item._1));
		
		StatCounter stats_npatterns = rdd_npatterns.stats();
		Tuple2<double[], long[]> histogram = rdd_npatterns.histogram(4);
		
		// Récupère la médiane
		JavaPairRDD<Double, Long> rdd_npatterns_index = rdd_npatterns.zipWithIndex();
		long rddSize = rdd_npatterns_index.count();
		
		Double median;
		if (rddSize % 2 == 0) {
			List<Double> middleElements = rdd_npatterns_index.filter(item -> (item._2 == rddSize/2 || item._2 == rddSize/2-1))
					   .map(item -> item._1)
					   .collect();

			median = (middleElements.get(0) + middleElements.get(1)) / 2;
		}
		else {
			median = rdd_npatterns_index.filter(item -> (item._2 == rddSize/2))
					   .map(item -> item._1)
					   .first();
		}
		
		/*
		System.out.println("WITH COLLECT");
		List<Tuple2<Double, Long>> list = rdd_npatterns_index.collect();
		System.out.println(list.get(0));
		if (rddSize % 2 == 0) {
			Double d = (list.get(list.size()/2)._1 + list.get(list.size()/2-1)._1) / 2;
			System.out.println(d);
		}
		else {
			System.out.println(list.get(list.size()/2));
		}
		System.out.println(list.get(list.size()-1));
		*/
		
		System.out.println("Distribution de npatterns: ");
		System.out.println("Min : " + stats_npatterns.min());
		System.out.println("Max : " + stats_npatterns.max());
		System.out.println("Moyenne : " + stats_npatterns.mean());
		System.out.println("Médiane: " + median);
		//System.out.println("Premier quadrant : " + stats_npatterns.mean());
		//System.out.println("Troisieme quadrant : " + stats_npatterns.mean());
		
		System.out.println("Histogramme : ");
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
			
			System.out.println(histogramString.toString());
		}
		
		context.close();
		
	}
	
}
