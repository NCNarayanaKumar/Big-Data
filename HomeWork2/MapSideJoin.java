

import java.io.*;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


@SuppressWarnings("deprecation")
public class MapSideJoin{
	
	
	public static class mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		HashMap<String,String> userMap = new HashMap<String,String>();
		
		protected void setup(Context context) throws IOException{
			
			try{
				Path[] path = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				File myFile = new File(path[0].getName());
				//System.out.println("File is "+ myFile);
					loadUserMap(myFile,context);
				}catch(IOException e){
					e.printStackTrace();
				}
		}
		
		public void loadUserMap(File filepath,Context context) throws IOException{
			
			BufferedReader breader = new BufferedReader(new FileReader(filepath.toString()));
			String line;
			
			try{
			while((line = breader.readLine())!= null){
				String[] split = line.split("::");
				userMap.put(split[0], split[1]);
				}
			}
			catch(FileNotFoundException e){
				e.printStackTrace();
			}
			catch(IOException e){
				e.printStackTrace();
			}
			finally{
				if((breader.readLine())==null){
					breader.close();
				}
			}
		}
		
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			Configuration conf = context.getConfiguration();
			String movieid = conf.get("movieid");
			String line = value.toString();
			String[] split = line.split("::");
			Text outputkey=new Text(""); 
			final IntWritable outputValue = new IntWritable(1);
			
			String gender = userMap.get(split[0].toString());
			outputkey.set(split[1].toString());
			if((outputkey.toString()).equals(movieid)&&gender.equals("M")){
				context.write(outputkey, outputValue);
			}

		}
		
	}
	
	public static class reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        context.write(key, new IntWritable(sum));

		}
		
	}
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		conf.set("movieid", args[3]);
		Job job = new Job(conf,"MapSideJoin");
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}

