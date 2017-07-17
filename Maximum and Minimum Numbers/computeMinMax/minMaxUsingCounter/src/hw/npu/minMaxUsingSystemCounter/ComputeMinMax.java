package hw.npu.minMaxUsingSystemCounter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComputeMinMax {

	public static class Map extends
			Mapper<LongWritable, Text, Text, MinMaxWritable> {

		private MinMaxWritable minMaxWritable = new MinMaxWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int firstVal;

			String[] lineSplit = value.toString().split("#");

			try {
				firstVal = Integer.parseInt(lineSplit[0]);
				minMaxWritable.setMin(firstVal);
				minMaxWritable.setMax(firstVal);

				for (String string : lineSplit) {
					// find the min and max and set
					try {
						int intWord = Integer.parseInt(string);

						if (intWord < minMaxWritable.getMin())
							minMaxWritable.setMin(intWord);
						if (intWord > minMaxWritable.getMax())
							minMaxWritable.setMax(intWord);
					} catch (Exception ex) {
						minMaxWritable.setCountInvalid(1);
						// System.out.println("First");
					}
				}
			} catch (Exception ex) {
				minMaxWritable.setCountInvalid(1);
				// System.out.println("2nd");
			}
			// System.out.println("Result Min "+ minMaxWritable.getMin());
			// System.out.println("Result Max "+ minMaxWritable.getMax());

			context.write(new Text("MinMax"), minMaxWritable);
		}

	}

	public static class Reduce extends
			Reducer<Text, MinMaxWritable, Text, MinMaxWritable> {

		private MinMaxWritable result = new MinMaxWritable();

		public void reduce(Text key, Iterable<MinMaxWritable> values,
				Context context) throws IOException, InterruptedException {

			int sumInvalid = 0;

			for (MinMaxWritable val : values) {

				if (val.getCountInvalid() < 1) {
					result.setMin(val.getMin());
					result.setMax(val.getMax());
					break;
				}
			}

			for (MinMaxWritable val : values) {

				if (val.getCountInvalid() < 1) {
					if (val.getMin() < result.getMin())
						result.setMin(val.getMin());
					if (val.getMax() > result.getMax())
						result.setMax(val.getMax());
				} else {
					sumInvalid += 1;
					// System.out.println("sumInValid "+sumInvalid);
				}
			}
			result.setCountInvalid(sumInvalid);
			// System.out.println("Result Min reduce "+ result.getMin());
			// System.out.println("Result Max reduce "+ result.getMax());
			// System.out.println("Invalid lines reduce "+
			// result.getCountInvalid());

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "ComputeMinMax");
		job.setJarByClass(ComputeMinMax.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

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
