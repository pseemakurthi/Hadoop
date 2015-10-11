

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	MultipleOutputs mos=null;
	int topScore=0;
	ArrayList<Text> topScorers;
	
	protected void setup(Context context) throws IOException ,InterruptedException {
		System.out.println("MyReducer.setup(-,-)");
		topScorers=new ArrayList<Text>();
		mos=new MultipleOutputs(context);
	};
	
	/*
	980 	mahesh,SMCS,Orissa
	980		Ratan ,SMCS,Andhra Pradesh 
	980 [{mahesh,SMCS,Orissa},{Ratan ,SMCS,Andhra Pradesh}]*/
	
	@Override
    public void reduce(IntWritable currentMark, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    	    	    	
    	if(currentMark.get()>topScore){
    		topScore=currentMark.get();
    		topScorers.clear();//Removes all Elements from the list.
    		//Note: In the for each loop,if you dont create a new Text object,
    		//and if you write the same object,then hadoop will preserve the lately 
    		//store element for 10 times.
    		for(Text student:values){
    			topScorers.add(new Text(student));
    			//topScorers.add(student); //try this for magic
    		}
    		System.out.println("current topScorer with Marks"+currentMark+topScorers);
    	}
	}
    	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		System.out.println("MyReducer.cleanup(-)");
		System.out.println(topScorers);
		for (Text studentDetails : topScorers) {
			// use mos to give meaningful name to output file instead of
			// part-r-000000
			mos.write(topScore, studentDetails, "studentMarks.txt");

		}
		mos.close();
	}
}