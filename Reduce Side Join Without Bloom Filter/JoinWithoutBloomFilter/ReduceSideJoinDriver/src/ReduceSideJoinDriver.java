import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoinDriver {

	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformUsersToMap(value
					.toString());
			String userId = parsed.get("Id");
			if (userId == null) {
				return;
			}
			outkey.set(userId);
			outvalue.set("A" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class CommentJoinMapper extends
			Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			String userId = parsed.get("UserId");
			if (userId == null) {
				return;
			}
			outkey.set(userId);
			outvalue.set("B" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		private String joinType = null;

		@Override
		public void setup(Context context) {
			joinType = context.getConfiguration().get("join.type");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			listA.clear();
			listB.clear();
			for (Text t : values) {
				if (t.charAt(0) == 'A') {
					listA.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'B') {
					listB.add(new Text(t.toString().substring(1)));
				}
			}
			executeJoinLogic(context);
		}

		private void executeJoinLogic(Context context) throws IOException,
				InterruptedException {
			if (joinType.equalsIgnoreCase("inner")) {
				if (!listA.isEmpty() && !listB.isEmpty()) {
					for (Text A : listA) {
						for (Text B : listB) {
							context.write(A, B);
						}
					}
				}
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				for (Text A : listA) {
					if (!listB.isEmpty()) {
						for (Text B : listB) {
							context.write(A, B);
						}
					} else {
						context.write(A, new Text(""));
					}
				}
			} else if (joinType.equalsIgnoreCase("rightouter")) {
				for (Text B : listB) {
					if (!listA.isEmpty()) {
						for (Text A : listA) {
							context.write(A, B);
						}
					} else {
						context.write(new Text(""), B);
					}
				}
			} else if (joinType.equalsIgnoreCase("fullouter")) {
				if (!listA.isEmpty()) {
					for (Text A : listA) {
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(A, B);
							}
						} else {
							context.write(A, new Text(""));
						}
					}
				} else {
					for (Text B : listB) {
						context.write(new Text(""), B);
					}
				}
			} else if (joinType.equalsIgnoreCase("anti")) {
				if (listA.isEmpty() ^ listB.isEmpty()) {
					for (Text A : listA) {
						context.write(A, new Text(""));
					}
					for (Text B : listB) {
						context.write(new Text(""), B);
					}
				}
			} else {
				throw new RuntimeException(
						"Join type not set to inner, leftouter, rightouter, fullouter, or anti");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: ReduceSideJoin <user data> <comments data> <out> [inner|leftouter|rightouter|fullouter|anti]");
			System.exit(1);
		}
		String joinType = otherArgs[3];
		if (!(joinType.equalsIgnoreCase("inner")
				|| joinType.equalsIgnoreCase("leftouter")
				|| joinType.equalsIgnoreCase("rightouter")
				|| joinType.equalsIgnoreCase("fullouter") || joinType
					.equalsIgnoreCase("anti"))) {
			System.err
					.println("Join type not set to inner, leftouter,rightouter, fullouter, or anti");
			System.exit(2);
		}
		Job job = new Job(conf, "Reduce Side Join");
		job.getConfiguration().set("join.type", joinType);
		job.setJarByClass(ReduceSideJoinDriver.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, UserJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, CommentJoinMapper.class);
		job.setReducerClass(UserJoinReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}

}
