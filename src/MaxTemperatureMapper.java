import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable>
{
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException
	{
		String line = value.toString();
		String[] data = line.split(",");
		String monthDay = line.substring(10, 15);
		int airTemperature;
		String temp = "";
		temp = data[2].trim();
		airTemperature = (int)Double.parseDouble(temp);
		output.collect(new Text(monthDay), new IntWritable(airTemperature));
	}
}