package bigdata;

import java.util.List;

import org.apache.hadoop.io.NullWritable;
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
	
	private static String inputPath = "/user/nsentout/output/part-m-00493";

	public static void main(String[] args)
	{	
		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//String inputPath = "hdfs://froment:9000" + args[0];
		
		JavaRDD<PhaseWritable> phasesRdd = context.sequenceFile(inputPath, NullWritable.class, PhaseWritable.class)
				.values().persist(StorageLevel.MEMORY_AND_DISK());

		JavaRDD<Long> nonIdlePhases = phasesRdd.filter(f ->
		!f.getPatterns().equals("-1")).map(f -> f.getDuration());

		context.close();
         
         //appendToOutput("NON IDLE PHASES");
         //calculateStats(conf, nonIdlePhases);
         //writeToFile(context, args[1] + "nonidle");
	}
	
}

