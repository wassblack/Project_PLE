import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceDistribution
{
	public static class TP4Mapper extends Mapper<LongWritable, Text, NullWritable, DistributionWritable>
	{
		public int nb_step = 0;
		
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			nb_step = conf.getInt("steps", 10);
			System.out.println("STEPS: " + nb_step);
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException 
		{
			// Retourne car la première ligne correspond aux noms des colonnes
			if (key.get() == 0) return;
			
			String tokens[] = value.toString().split(";");
			
			boolean isIdle = tokens[3].equals("-1");
			
			if (!isIdle) {
				String npatternsToken = tokens[4];
				
				// Si npatterns est renseignée
				if (npatternsToken.length() != 0)
				{
					int npatterns = Integer.parseInt(npatternsToken);
					DistributionWritable outValue = new DistributionWritable(1, npatterns);
					
					context.write(NullWritable.get(), outValue);
				}
			}
		}
	}
	/*
	public static class HistogramCombiner extends Reducer<NullWritable, DistributionWritable, NullWritable, DistributionWritable>
	{
		public void reduce(NullWritable key, Iterable<DistributionWritable> values, Context context) throws IOException, InterruptedException
		{
			int partialSum = 0;
			int partialCount = 0;
			int partialMin = Integer.MAX_VALUE;
			int partialMax = 0;
			
			for (DistributionWritable v : values) {
				partialSum += v.sum;
				partialCount += v.count;
				partialMin = Math.min(partialMin, v.min);
				partialMax = Math.max(partialMax, v.max);
			}
			
			DistributionWritable partialCity = new DistributionWritable(partialCount, partialSum, partialMin, partialMax);
			
			context.write(NullWritable.get(), partialCity);
		}
	}
	*/
	public static class TP4Reducer extends Reducer<NullWritable, DistributionWritable, NullWritable, Text>
	{
		public int nb_step = 0;
		private ArrayList<Integer> npatternsList = new ArrayList<Integer>();
		private Text outValue = new Text();
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			nb_step = conf.getInt("steps", 10);
			System.out.println("steps: " + nb_step);
		}
		
		public void reduce(NullWritable key, Iterable<DistributionWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int totalSum = 0;
			int totalCount = 0;
			int min = Integer.MAX_VALUE;
			int max = 0;
			int median = 0;
			
			for (DistributionWritable v : values) {
				totalSum += v.sum;
				totalCount += v.count;
				min = Math.min(min, v.min);
				max = Math.max(max, v.max);
				npatternsList.add(v.sum);
			}
			
			Collections.sort(npatternsList);
		
			try {
			if (totalCount % 2 == 0) {
				int middleNumber1 = npatternsList.get(totalCount / 2);
				int middleNumber2 = npatternsList.get(totalCount / 2 - 1);
				median = (middleNumber1 + middleNumber2) / 2;
			}
			else {
				median = npatternsList.get(totalCount / 2);
			}
			}
			catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
			
			double avg = (double)totalSum/(double)totalCount;
			StringBuilder result = new StringBuilder();
			result.append("Min : " + min + "\n");
			result.append("Max : " + max + "\n");
			result.append("Moyenne : " + avg + "\n");
			result.append("Median : " + median + "\n");
			
			outValue.set(result.toString());
			
			context.write(NullWritable.get(), outValue);
			
			//context.write(NullWritable.get(), new Text(outValue));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		try {
			conf.setInt("steps", Integer.parseInt(args[2]));
		} 
		catch (Exception e) {
			System.out.println("bad arguments, waiting for 3 arguments [inputURI][outputURI][NB_STEPS]");
		}
		
		Job job = Job.getInstance(conf, "Projet - MapReduce tests");
		job.setNumReduceTasks(1);
		job.setJarByClass(MapReduceDistribution.class);
		//job.setCombinerClass(HistogramCombiner.class);
		job.setMapperClass(TP4Mapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DistributionWritable.class);
		job.setReducerClass(TP4Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
