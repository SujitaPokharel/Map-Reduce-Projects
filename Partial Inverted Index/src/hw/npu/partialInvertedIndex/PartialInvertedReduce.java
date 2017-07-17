package hw.npu.partialInvertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import hw.npu.partialInvertedIndex.DocSumWritable;


public class PartialInvertedReduce extends MapReduceBase implements
Reducer<Text, Text, Text, DocSumWritable> {
private HashMap<String, Integer> map;
private void add(String tag) {
Integer val;

if (map.get(tag) != null) {
	val = map.get(tag);
	map.remove(tag);
} else {
	val = 0;
}

map.put(tag, val + 1);
}

@Override
public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DocSumWritable> output, Reporter reporter)
	throws IOException {
map = new HashMap<String, Integer>();

while (values.hasNext()) {
	add(values.next().toString());
}

output.collect(key, new DocSumWritable(map));

}
}
