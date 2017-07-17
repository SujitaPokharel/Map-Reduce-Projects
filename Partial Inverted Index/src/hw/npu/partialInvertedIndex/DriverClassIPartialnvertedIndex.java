package hw.npu.partialInvertedIndex;

import java.util.concurrent.Callable;
import javax.xml.soap.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import hw.npu.partialInvertedIndex.DocSumWritable;

public class DriverClassIPartialnvertedIndex implements Callable<String> {

	private String inputPaths;
	private String outputPath;
	private RunningJob runningJob;

	public void setInputPaths(String inputPaths) {
		this.inputPaths = inputPaths;
	}

	public String getInputPaths() {
		return inputPaths;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public RunningJob getRunningJob() {
		return runningJob;
	}

	public String call() throws Exception {

		JobConf job = new JobConf();
		job.setJarByClass(getClass());

		initJobConf(job);
		initCustom(job);
		FileInputFormat.setInputPaths(job, getInputPaths());

		if (getOutputPath() != null)
			FileOutputFormat.setOutputPath(job, new Path(getOutputPath()));

		JobClient client = new JobClient(job);
		this.runningJob = client.submitJob(job);
		return runningJob.getID().toString();
	}

	public static void initCustom(JobConf conf) {
		conf.setOutputValueClass(DocSumWritable.class);
	}

	public static void initJobConf(JobConf conf) {

		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		conf.setMapperClass(hw.npu.partialInvertedIndex.PartialinvertedMap.class);
		conf.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		conf.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
		conf.setPartitionerClass(org.apache.hadoop.mapred.lib.HashPartitioner.class);
		conf.setOutputKeyComparatorClass(org.apache.hadoop.io.Text.Comparator.class);
		conf.setReducerClass(hw.npu.partialInvertedIndex.PartialInvertedReduce.class);
		conf.setNumReduceTasks(1);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
	}

	public static void main(String[] args) throws Exception {
		DriverClassIPartialnvertedIndex job = new DriverClassIPartialnvertedIndex();
		if (args.length >= 1)
			job.setInputPaths(args[0]);
		if (args.length >= 2)
			job.setOutputPath(args[1]);
		job.call();
		job.getRunningJob().waitForCompletion();
	}
}
