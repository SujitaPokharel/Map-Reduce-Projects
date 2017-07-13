package Hw02;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComputeAverage {

	public static class MapClass extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
			Integer inputArray[];

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			String[] line = value.toString().split(" ");
			ArrayList<Integer> listInteger = new ArrayList<Integer>();
			for (String string : line) {
				listInteger.add(Integer.parseInt(string));
			}

			int summation = 0;

			for (int elements : listInteger)
				summation += elements;

			float avg = (float) ((summation * 1.0) / listInteger.size());
			context.write(new Text("Average"), new FloatWritable(avg));
		}
	}

	public static class ReduceClass extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {

			float totalAvg = 0;
			int count = 0;
			for (FloatWritable val : values) {

				totalAvg += val.get();
				count++;
			}
			
			float combinedAvg = totalAvg / count;
			context.write(new Text("Average"), new FloatWritable(combinedAvg));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "ComputeAverage");
		job.setJarByClass(ComputeAverage.class);

		System.out.println("Started");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job,
				new Path(args[0]), TextInputFormat.class);
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job,
				new Path(args[1]), TextInputFormat.class);
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job,
				new Path(args[2]), TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}
