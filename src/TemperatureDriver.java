import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TemperatureDriver
{
	public static void main(String[] args) throws Exception
	{
		JobClient myClient = new JobClient();
		//Crete a configuration object for the job
		JobConf jobConf = new JobConf(TemperatureDriver.class);
		
		//Set a name of the Job
		jobConf.setJobName("MaxTemp");
		
		//Specify data type of output key and value
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		//Specify names of Mapper and Reducer class
		jobConf.setMapperClass(MaxTemperatureMapper.class);
		jobConf.setReducerClass(MaxTemperatureReducer.class);
		
		//Specify formats of the data type of Input and output
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		
		//Set input and output directories using command line arguments
		//arg[0] = name of input directory on HDFS and arg[1] = name of output directory to be created
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		myClient.setConf(jobConf);
		try
		{
			//run the job
			JobClient.runJob(jobConf);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}