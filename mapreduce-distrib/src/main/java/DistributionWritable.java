import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DistributionWritable implements Writable
{
	public int sum;
	public int count;
	
	// Population min
	public int min;
	// Population max
	public int max;
	
	// Sinon erreur
	public DistributionWritable() {}
	
	public DistributionWritable(int count, int sum)
	{
		this.count = count;
		this.sum = this.max = this.min = sum;
	}
	
	public DistributionWritable(int count, int sum, int min, int max)
	{
		this.count = count;
		this.sum = sum;
		this.min = min;
		this.max = max;
	}

	public void readFields(DataInput in) throws IOException
	{
		sum = in.readInt();
		count = in.readInt();
		max = in.readInt();
		min = in.readInt();
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeInt(sum);
		out.writeInt(count);
		out.writeInt(max);
		out.writeInt(min);
	}

}
