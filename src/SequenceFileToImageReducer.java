import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SequenceFileToImageReducer extends
		Reducer<Text, Text, Text, Text> {
	
	private MultipleOutputs mos;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs(context);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
	
	public void reduce(Text key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		String filename = getFilenameFromKey(key);
		key.set(key.toString().split("_r_")[0]);
		filename = context.getConfiguration().get("BASE_OUTPUT_FILE_NAME")
				+ "n" + filename;
		for (BytesWritable value : values) {
			mos.write(key, value, filename);
		}
	}

	private String getFilenameFromKey(Text key) {
		String a[] = key.toString().split("_r_");
		return a[a.length - 1];
	}

}

class SequenceFilePartitioner extends Partitioner<Text, BytesWritable> {

	@Override
	public int getPartition(Text key, BytesWritable value, int numReduceTasks) {

		String a[] = key.toString().split("_r_");
		//logger.info("sending " + key.toString() + " to " + a[a.length - 1]);
		return Integer.parseInt(a[a.length - 1]);
	}
}
