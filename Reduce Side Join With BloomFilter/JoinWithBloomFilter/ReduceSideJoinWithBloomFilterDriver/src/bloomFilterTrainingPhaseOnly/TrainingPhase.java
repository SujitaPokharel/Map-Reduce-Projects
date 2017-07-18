package bloomFilterTrainingPhaseOnly;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class TrainingPhase {
	public static class UserMapperWithBloom extends
			Mapper<Object, Text, Text, NullWritable> {
		private Text outkey = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");
			if (userId == null || reputation == null) {
				return;
			}
			if (Integer.parseInt(reputation) > 1500) {
				outkey.set(parsed.get("Id"));
				context.write(outkey, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: BloomFilterWriter <inputfile> <outputfile> <nummembers> <falseposrate> <bfoutfile>");
			System.exit(1);
		}
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(TrainingPhase.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(UserMapperWithBloom.class);
		job.setReducerClass(Reducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		Path inputFile = new Path(otherArgs[1]);		
		int numMembers = Integer.parseInt(otherArgs[2]);
		float falsePosRate = Float.parseFloat(otherArgs[3]);
		Path bfFile = new Path(otherArgs[4]);
		
		int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
		int nbHash = getOptimalK(numMembers, vectorSize);
		
		BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
		System.out.println("Training Bloom filter of size " + vectorSize + " with " + nbHash + " hash functions, " + numMembers
				+ " approximate number of records, and " + falsePosRate + " false positive rate");
		String line = null;
		int numRecords = 0;
		for (FileStatus status : fs.listStatus(inputFile)) {
			BufferedReader rdr;
			if (status.getPath().getName().endsWith(".gz")) {
				rdr = new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(status.getPath()))));
			} else {
				rdr = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
			}
			System.out.println("Reading " + status.getPath());
			while ((line = rdr.readLine()) != null) {
				filter.add(new Key(line.getBytes()));
				++numRecords;
			}
			rdr.close();
		}
		System.out.println("Trained Bloom filter with " + numRecords + " entries.");
		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();
		System.out.println("Done training Bloom filter.");
	}

	public static int getOptimalBloomFilterSize(int numRecords,float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

}
