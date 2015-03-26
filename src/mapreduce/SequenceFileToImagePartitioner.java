package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SequenceFileToImagePartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {

		String a[] = key.toString().split("_r_");
		// logger.info("sending " + key.toString() + " to " + a[a.length - 1]);
		return Integer.parseInt(a[a.length - 1]);
	}
}
