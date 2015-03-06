import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SequenceFileToImageReducer extends
		Reducer<Text, Text, Text, Text> {
	
	private MultipleOutputs mos;
	private static Log logger = LogFactory
			.getLog(SequenceFileToImageMapper.class);

	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs(context);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		String filename = getFilenameFromKey(key);
		key.set(key.toString().split("_r_")[0]);
		filename = context.getConfiguration().get("BASE_OUTPUT_FILE_NAME")
				+ "n" + filename;
		int l = 0;
		
		for (Text value : values) {
			l++;
			mos.write(key, value, filename);
		}
		logger.info("received" +  l + " values for key " + key);
	}

	private String getFilenameFromKey(Text key) {
		String a[] = key.toString().split("_r_");
		return a[a.length - 1];
	}

}

class SequenceFilePartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {

		String a[] = key.toString().split("_r_");
		//logger.info("sending " + key.toString() + " to " + a[a.length - 1]);
		return Integer.parseInt(a[a.length - 1]);
	}
}
