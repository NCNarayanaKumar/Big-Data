import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ImdbAnalytics1 {

	
	public static class ImdbMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			Configuration conf = context.getConfiguration();
            String zip = conf.get("zipcode");
			
			//System.out.println(zip);
			String line = value.toString();
			String[] split = line.split("::");
			if(split[4].equals(zip)){
				Text outputkey = new Text(split[0]);
				Text outputvalue = new Text(split[4]);
				context.write(outputkey,outputvalue);
			}
			
		}
	
	}
	
	public static class ImdbReducer extends Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Text value, Context context) 
	      throws IOException, InterruptedException {
	   
	        context.write(key, value);
	    }
	 }
	
	public static void main(String[] args) throws Exception {
        
		Configuration conf = new Configuration();

		   conf.set("zipcode", args[2]);
 
        // Create a job for the map reduce task
        @SuppressWarnings("deprecation")
		Job job = new Job(conf,"ImdbAnalytics1");
 
        // set the mapper and reducer class
        job.setMapperClass(ImdbMapper.class);
        job.setReducerClass(ImdbReducer.class);
 
        // Set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        // Wait for the job to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
