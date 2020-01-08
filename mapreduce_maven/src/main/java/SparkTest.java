
import java.util.ArrayList;
import java.util.List;
import java.lang.Long;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;


public class SparkTest {
	
	private static String inputPath = "/user/nsentout/PhasesSequenceFile/part-m-0043*";

	public static void main(String[] args)
	{	
		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<PhaseWritable> phasesRdd = context.sequenceFile(inputPath, NullWritable.class, PhaseWritable.class)
				.values().persist(StorageLevel.MEMORY_AND_DISK());

		JavaRDD<Long> nonIdlePhases = phasesRdd.filter(f -> !f.getPatterns().equals("-1"))
					.map(f -> f.getDuration());
		
		ArrayList<String> output = new ArrayList<String>();
		SparkTest.computeStats(nonIdlePhases, output);
		
		context.parallelize(output).saveAsTextFile("hdfs://froment:9000/user/nsentout/output-project");
	}
	
	private static void computeStats(JavaRDD<Long> rdd, ArrayList<String> output)
	{
		JavaDoubleRDD rddDouble = rdd.mapToDouble(f -> f);
		StatCounter statistics = rddDouble.stats();
		
		output.add("min :"+statistics.min());
		output.add("max :"+statistics.max());
		output.add("avg :"+statistics.mean());
		output.add("total :"+statistics.sum());
		
		// Récupère la médiane
		JavaPairRDD<Long, Long> rdd_npatterns_index = rdd.sortBy(f -> f, true, rdd.getNumPartitions())
					.zipWithIndex().mapToPair(f -> new Tuple2<>(f._2, f._1));
		
		long count = rdd_npatterns_index.count();
		
		long median = 0;
		if (count % 2 == 0) {
			long left = ((long) count / 2) - 1;
			long right = left + 1;
			median = (rdd_npatterns_index.lookup(left).get(0) + rdd_npatterns_index.lookup(right).get(0)) / 2;
		}
		else {
			median = rdd_npatterns_index.lookup(count / 2).get(0);
		}
		
		output.add("median :"+median);
		
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
	}
	
}

