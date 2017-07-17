import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PiCalculation {
	public static class MyMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static int counter = 0;
		private static double radius = 0;
		private static double numOfPair = 0;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedIOException,
				InterruptedException {

			String line = value.toString();

			if (line != null) {
				String[] elements = line.split(",");
				if (counter < 1) {
					radius = Double.valueOf(elements[0]);
					numOfPair = Double.valueOf(elements[1]);
					counter++;
				} else {
					double xCoordinate = Double.valueOf(elements[0]);
					double yCoordinate = Double.valueOf(elements[1]);
					System.out.println("Radius: " + radius);
					/*
					 * System.out.println("xCoordinate: "+ xCoordinate);
					 * System.out.println("yCoordinate: "+ yCoordinate);
					 */
					if ((Math.pow(xCoordinate, 2) + Math.pow(yCoordinate, 2)) < Math
							.pow(radius, 2)) {
						System.out.println("Inside 1");
						context.write(new Text("Inside"), one);
					} else {
						System.out.println("Outside 1");
						context.write(new Text("Outside"), one);
					}

				}

			}

		}

	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Job job = new Job(config, "Pi Job");

		job.setJarByClass(PiCalculation.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
