import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordFirstCharCnt {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				char initLetter = word.toString().charAt(0);
				
				if(Character.isLetter(initLetter)){
					initLetter = Character.toLowerCase(initLetter);
					word.set(""+initLetter);
					context.write(word, one);
				}
			}
		}
	}
		
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
		
	public static void main(String[] args) throws Exception{
		
		Configuration config = new Configuration();
		Job job = new Job(config, "WordFirstCharCnt");
		job.setJarByClass(WordFirstCharCnt.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);// map function
		job.setCombinerClass(Reduce.class);//combine function
		job.setReducerClass(Reduce.class);//reduce function
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path (args[1]));
		
		job.waitForCompletion(true);
		
		
	}
}
