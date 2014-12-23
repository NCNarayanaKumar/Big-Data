import java.io.*;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ImdbAnalytics2 {
	
	public static class ImdbMapper1 extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			String[] split = line.split("::");
				Text outputkey = new Text(split[1]);
				FloatWritable outputvalue = new FloatWritable(Float.parseFloat(split[2]));
				context.write(outputkey,outputvalue);
			
		}
	
	}
	
	public static class ImdbReducer1 extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	    public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
	      throws IOException, InterruptedException {
	    	float sum=0;
	    	int i=0;
	        for (FloatWritable val : values) {
	            sum += val.get();
	            i+=1;
	        }
	        float avg = sum/i;
	        FloatWritable value = new FloatWritable(avg);
	        context.write(key, value);
	    }
	 }
	
 public static class ImdbMapper2 extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			float dev = 5;
			String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
			Text value2 = new Text(tokenizer.nextToken());
			Text key2 = new Text(Float.toString(dev-Float.parseFloat(tokenizer.nextToken())));
			context.write(key2,value2);
	        }
			
		}
	
	}
	
	public static class ImdbReducer2 extends Reducer<Text, Text, Text, FloatWritable> {

		int i=0;
		
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	float dev = 5;
	    	
	    	for (Text value : values) {
	    		if(i<10){
	    			FloatWritable key1 = new FloatWritable(dev-Float.parseFloat(key.toString()));
	    	    context.write(value, key1);
	    	    i=i+1;
	    		}
	    	}
	    }
	}

	
	
	
	public static void main(String[] args) throws Exception {
        
		Configuration conf1 = new Configuration();
 
        // Create a job for the map reduce task
        @SuppressWarnings("deprecation")
		Job job1 = new Job(conf1,"ImdbAnalytics2");
 
        // set the mapper and reducer class
        job1.setMapperClass(ImdbMapper1.class);
        job1.setReducerClass(ImdbReducer1.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);
 
        // Set the key and value class
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the input and output path
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        //job1.waitForCompletion(true);
 
        // Wait for the job to finish
       if(job1.waitForCompletion(true)){
        
        
        Configuration conf2 = new Configuration();
        
        // Create a job for the map reduce task
        @SuppressWarnings("deprecation")
		Job job2 = new Job(conf2,"ImdbAnalytics2");
 
        // set the mapper and reducer class
        job2.setMapperClass(ImdbMapper2.class);
        job2.setReducerClass(ImdbReducer2.class);
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
 
        // Set the key and value class
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the input and output path
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
 
        // Wait for the job to finish
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
        } 
    }

}
