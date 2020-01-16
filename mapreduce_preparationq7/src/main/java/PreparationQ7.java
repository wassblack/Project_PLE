import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PreparationQ7 {
	
	private static final int NUMBER_PATTERNS = 24;
	private static final char PATTERN_DELIMITER_OUTPUT_FILE = 'p';
	
	public static List<String> getFourCombinations(String[] patterns, char delimiter) {
		List<String> combinations = new ArrayList<String>();
		
		for (int i = 0; i < patterns.length; i++) {
			for (int j = i+1; j < patterns.length; j++) {
				for (int k = j+1; k < patterns.length; k++) {
					for (int l = k+1; l < patterns.length; l++) {
						combinations.add(patterns[i] + delimiter + patterns[j] + delimiter + patterns[k] + delimiter + patterns[l]);
					}
				}
			}
		}
		
		return combinations;
	}
	
	public static class Question7Mapper extends Mapper<Object, Text, Text, Text>
	{
		Text outKey = new Text();
		Text outValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] phase = value.toString().split(";");
			
			if(!phase[0].equals("start")) {
				String[] patterns = phase[3].split(",");

				if (patterns.length >= 4) {
					List<String> combinations = getFourCombinations(patterns, PATTERN_DELIMITER_OUTPUT_FILE);
					outValue.set("[" + phase[0] + "," + phase[1] + "]");

					for (String combination : combinations) {
						outKey.set(combination);
						context.write(outKey, outValue);
					}
				}	
			}
		}
	}
	
	public static class Question7Reducer extends Reducer<Text, Text, Text, NullWritable>
	{
		MultipleOutputs<Text, Text> multipleOutputs;
		
		@Override
		public void setup(Context context) {
			multipleOutputs = new MultipleOutputs(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			String outputFileName = key.toString().replace(',', PATTERN_DELIMITER_OUTPUT_FILE);
			
			for (Text value : values) {
				multipleOutputs.write(outputFileName, value, NullWritable.get());
			}
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		conf.set("mapreduce.map.memory.mb", "512");
		conf.set("mapreduce.reduce.memory.mb", "4096");
		
		Job job = Job.getInstance(conf, "PhaseCreator");
		//job.setNumReduceTasks(10);
		job.setJarByClass(PreparationQ7.class);
		
		job.setMapperClass(Question7Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Question7Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		String[] allPatterns = new String[NUMBER_PATTERNS];
		for (int i = 0; i < NUMBER_PATTERNS; i++) {
			allPatterns[i] = String.valueOf(i);
		}
		
		List<String> combinations = getFourCombinations(allPatterns, PATTERN_DELIMITER_OUTPUT_FILE);
		
		for (String combination : combinations) {
			MultipleOutputs.addNamedOutput(job, combination, TextOutputFormat.class, Text.class, NullWritable.class);
		}
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
