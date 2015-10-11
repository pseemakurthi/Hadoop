

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	MultipleOutputs mos=null;
	MyReducer(){
		System.out.println("MyReducer()");
	}
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		System.out.println("MyReducer.setUp(-)");
		mos=new MultipleOutputs(context);
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		System.out.println("MyReducer.cleanup(-)");
		mos.close();
	}
	
	//apple [1,1,1,1,1,1]
	//orange[1,1,1]
	
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
    	System.out.println("reduce(-,-,-)");
    	
        int sum = 0;
        for (IntWritable value : values) {
        	System.out.print(" "+value.get());
            sum += value.get();
        }
        System.out.println();
        mos.write(key, new IntWritable(sum),"abc.txt");
    }
}