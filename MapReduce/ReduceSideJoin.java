import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoin {
	
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
	
 public static class ImdbMapper2A extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
			Text key2 = new Text(tokenizer.nextToken());
			Text value2 = new Text(tokenizer.nextToken()+"Rating");
			context.write(key2,value2);
	        }
			
		}
	
	}
 
 public static class ImdbMapper2B extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String split[] = line.split("::");
			Text outputKey = new Text(split[0]);
			Text outputValue = new Text();
			outputValue.set(split[1]);
			context.write(outputKey, outputValue);
	        }
			
		}
	
	
	public static class ImdbReducer2 extends Reducer<Text, Text, Text, Text> {
	    
		
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	
	    	float dev = 5;
	    	
	    	Text joinkey = new Text();
	    	Text joinvalue = new Text();
	    	for (Text value : values) {
	    		if(value.toString().contains("Rating")){
	    			joinkey = new Text(Float.toString(dev-Float.parseFloat(value.toString().replace("Rating",""))));
	    		}else{
	    			joinvalue.set(value);
	    		}
	    		
	    	}	
	    	Text finalkey = new Text("1");
	    	Text finalvalue = new Text();
	    	finalvalue.set("::"+joinkey+"::"+joinvalue);
	    	context.write(finalkey,finalvalue);
	    	 }
	    

	}
	
	 public static class ImdbMapper3 extends Mapper<LongWritable, Text, Text, Text>{
			
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
				String line = value.toString();
				String[] split = line.split("::");
				if(split[1].equals("")){
				}else{
				context.write(new Text(split[1]), new Text(split[2]));
		        }
			}
			}
	
public static class ImdbReducer3 extends Reducer<Text, Text, Text, Text> {
	    
		
	int i=0;
	
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	float dev = 5;
    	
    	for (Text value : values) {
    		if(i<10){
    			Text key1 = new Text(Float.toString(dev-Float.parseFloat(key.toString())));
    	    context.write(key1, value);
    	   i=i+1;
    		}
    	}
    }
	    

	}

	
	public static void main(String[] args) throws Exception {
        
		Configuration conf1 = new Configuration();
 
        // Create a job for the map reduce task
        @SuppressWarnings("deprecation")
		Job job1 = new Job(conf1,"ReduceSideJoin");
 
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
       job1.waitForCompletion(true);
        
        
        Configuration conf2 = new Configuration();
        
        // Create a job for the map reduce task
        @SuppressWarnings("deprecation")
		Job job2 = new Job(conf2,"ReduceSideJoin");
 
        // set the mapper and reducer class
        job2.setMapperClass(ImdbMapper2A.class);
        job2.setMapperClass(ImdbMapper2B.class);
        job2.setReducerClass(ImdbReducer2.class);
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
 
        // Set the key and value class
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the input and output path
        
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, ImdbMapper2A.class);
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, ImdbMapper2B.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
 
        // Wait for the job to finish
        job2.waitForCompletion(true);
        	
        	@SuppressWarnings("deprecation")
    		Job job3 = new Job(conf1,"ReduceSideJoin");
     
            // set the mapper and reducer class
            job3.setMapperClass(ImdbMapper3.class);
            job3.setReducerClass(ImdbReducer3.class);
            
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
     
            // Set the key and value class
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            
            job3.setInputFormatClass(TextInputFormat.class);
            job3.setOutputFormatClass(TextOutputFormat.class);
            
            // Set the input and output path
            FileInputFormat.addInputPath(job3, new Path(args[3]));
            FileOutputFormat.setOutputPath(job3, new Path(args[4]));
            
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        
    }

}
