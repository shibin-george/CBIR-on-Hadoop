import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SeqReader {
	// private static Log logger = LogFactory
	// .getLog(BinaryFilesToHadoopSequenceFile.class);

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		if (args.length < 3) {
			System.out.println("Usage: <jar file> <sequence filename(s)> "
					+ "<desired number of output sequence files> "
					+ "<path to output folder>");
			System.exit(0);
		}

		int numOutputFiles = Integer.parseInt(args[args.length - 2]);
		if (numOutputFiles < 1) {
			// someone is screwing around
			numOutputFiles = 1;
		}

		DateFormat dateFormat = new SimpleDateFormat("ddMMyyyyHHmmss");
		Date date = new Date();
		String baseOutputName = "output" + dateFormat.format(date);

		Configuration conf = new Configuration();
		conf.set("BASE_OUTPUT_FILE_NAME", baseOutputName);
		conf.set("NUM_REDUCERS", Integer.toString(numOutputFiles));
		Job job = Job.getInstance(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SequenceFileToImageMapper.class);
		job.setReducerClass(SequenceFileToImageReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(numOutputFiles);
		
		for (int i = 0; i < args.length - 2; i++) {
			// FileInputFormat.setInputPaths(job, new Path(args[i]));
			MultipleInputs.addInputPath(job, new Path(args[i]),
					TextInputFormat.class);
		}
		for (int i = 0; i < numOutputFiles; i++) {
			MultipleOutputs.addNamedOutput(job,
					baseOutputName + "n" + Integer.toString(i),
					SequenceFileOutputFormat.class, Text.class,
					Text.class);
		}
		job.setJarByClass(SeqReader.class);
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		job.submit();
	}
}