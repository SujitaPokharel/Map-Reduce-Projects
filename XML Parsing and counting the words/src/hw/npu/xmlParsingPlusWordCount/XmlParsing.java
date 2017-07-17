package hw.npu.xmlParsingPlusWordCount;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;

public class XmlParsing {
	public static class MyMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			String valueText = parsed.get("Text");
			if (null != valueText) {
				StringTokenizer tokenizer = new StringTokenizer(valueText);
				while (tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken().toLowerCase());
					context.write(word, one);
				}

			}

		}
	}

	public static class MyReduce extends
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

	public static void main(String[] args) throws Exception,
			NullPointerException {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "XmlParsingWithWordCount");
		job.setJarByClass(XmlParsing.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyMap.class);
		job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}

}
