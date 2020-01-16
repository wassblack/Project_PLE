import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PhaseCreator {
	
	public static class PhaseCreatorMapper extends Mapper<Object, Text, NullWritable, PhaseWritable>
	{
		PhaseWritable outValue = new PhaseWritable();
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] phase = value.toString().split(";");
			
			if (!phase[0].equals("start")) {
				try {
					outValue.setStart(Long.parseLong(phase[0]));
					outValue.setDuration(Long.parseLong(phase[2]));
					outValue.setPatterns(phase[3]);
					outValue.setJobs(phase[5]);
					outValue.setDays(phase[7]);
				} catch (Exception e) {
					return;
				}
				context.write(NullWritable.get(), outValue);
			}
		}
	}



	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		conf.set("mapreduce.map.memory.mb", "1024");
		
		Job job = Job.getInstance(conf, "PhaseCreator");
		job.setNumReduceTasks(0);
		job.setJarByClass(PhaseCreator.class);
		
		job.setMapperClass(PhaseCreatorMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(PhaseWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PhaseWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
