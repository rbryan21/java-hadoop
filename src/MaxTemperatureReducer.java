import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureReducer extends MapReduceBase implements 
	Reducer<Text, IntWritable, Text, IntWritable>
{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException
	{
		System.out.println("In the reduce");
		int maxValue = Integer.MIN_VALUE;
		while (values.hasNext())
		{
			IntWritable value = (IntWritable) values.next();
			System.out.println("Value for " + key + " is " + value.get());
			maxValue = Math.max(maxValue, value.get());
		}
		output.collect(key, new IntWritable(maxValue));
	}

}
