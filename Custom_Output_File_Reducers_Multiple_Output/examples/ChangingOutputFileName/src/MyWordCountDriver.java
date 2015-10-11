import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCountDriver {

	public static void main(String[] args) throws Exception {

		Path input_dir = new Path("hdfs://localhost:54310/input_data");
		//A directory called result.txt will be created.It is not the file name
		Path output_dir = new Path(
				"hdfs://localhost:54310/output_data/result.txt");

		Configuration conf = new Configuration();

		Job job = new Job(conf, "MyWordCountJob");
		job.setJarByClass(MyWordCountDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(10);

		// specifying reducers output value
		// if you dont provide this,it assumes LongWritable,and then error
		job.setOutputKeyClass(Text.class);
		// if you dont provide this,it assumes Text,and hence error
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);
		output_dir.getFileSystem(conf).delete(output_dir, true);
		// This piece of code will actually intiate the Job run
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		// By default a folder is create output_data with 2 files _SUCCESS and
		// part-r-00000
	}
}
