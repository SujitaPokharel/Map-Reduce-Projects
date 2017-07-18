package project.npu.bloomFilter;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class BloomFilteringLookupPhase {

	public static class BloomFilteringMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private BloomFilter filter = new BloomFilter();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			URI[] files = DistributedCache.getCacheFiles(context
					.getConfiguration());
			if (files != null && files.length == 1) {
				System.out.println("Reading Bloom filter from: "
						+ files[0].getPath());
				DataInputStream strm = new DataInputStream(new FileInputStream(
						files[0].getPath()));
				filter.readFields(strm);
				strm.close();
			} else {
				throw new IOException(
						"Bloom filter file not set in the DistributedCache.");
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			String comment = parsed.get("Text");
			if (comment == null) {
				return;
			}
			StringTokenizer tokenizer = new StringTokenizer(comment);
			while (tokenizer.hasMoreTokens()) {
				String cleanWord = tokenizer.nextToken().replaceAll("'", "")
						.replaceAll("[^a-zA-Z]", " ");
				if (cleanWord.length() > 0
						&& filter.membershipTest(new Key(cleanWord.getBytes()))) {
					context.write(value, NullWritable.get());
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: BloomFiltering <in> <cachefile> <out>");
			System.exit(1);
		}
		FileSystem.get(conf).delete(new Path(otherArgs[2]), true);
		Job job = new Job(conf, "Bloom Filtering Lookup");
		job.setJarByClass(BloomFilteringLookupPhase.class);
		job.setMapperClass(BloomFilteringMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		DistributedCache.addCacheFile(
				FileSystem.get(conf).makeQualified(new Path(otherArgs[1]))
						.toUri(), job.getConfiguration());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
