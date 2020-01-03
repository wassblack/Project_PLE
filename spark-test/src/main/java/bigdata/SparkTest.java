package bigdata;

import java.util.ArrayList;
import java.util.List;
import java.lang.Long;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import org.apache.spark.storage.StorageLevel;

import scala.*;


public class SparkTest {
	
	private static String inputPath = "/user/wberrari/output/part-m-00493";

	public static void main(String[] args)
	{	
		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//String inputPath = "hdfs://froment:9000" + args[0];
		
		JavaRDD<PhaseWritable> phasesRdd = context.sequenceFile(inputPath, NullWritable.class, PhaseWritable.class)
				.values().persist(StorageLevel.MEMORY_AND_DISK());

		JavaRDD<Long> nonIdlePhases = phasesRdd.filter(f ->
		!f.getPatterns().equals("-1")).map(f -> f.getDuration());

		//StringBuilder stringBuilder = new StringBuilder();
		StatCounter statistics = nonIdlePhases.mapToDouble(f -> f).stats();
		ArrayList<String> output = new ArrayList<String>();
		output.add("min :"+statistics.min());
		output.add("max :"+statistics.max());
		output.add("avg :"+statistics.mean());
		output.add("total :"+statistics.sum());
		context.parallelize(output).saveAsTextFile("hdfs://froment:9000/nonidleResults");
		
		
		context.close();
         
	}
	
}

