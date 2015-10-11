

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        
    //mahesh,980,SMCS,Orissa,mahesh@gmail.com
	//  0     1    2    3		4
    //called once for each record.
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException
      {
    	//skip empty lines
    	if(line.toString().trim().length()==0)return ;
    	
    	//each record contains date in this format
    	//mahesh,980,SMCS,Orissa,mahesh@gmail.com
    	String studArr[]=line.toString().split(",");
    	
    	String studentName=studArr[0];
    	int mark=Integer.parseInt(studArr[1]);
    	String schoolName=studArr[2];
    	String state=studArr[3];
    	//mahesh,SMCS,Orissa
    	String value=studentName+","+schoolName+","+state;
    	
    	System.out.println("Mapper Output="+mark+":::"+value);
        context.write(new IntWritable(mark),new Text(value));//key we are using same,so that they can be grouped together
        
        }
}