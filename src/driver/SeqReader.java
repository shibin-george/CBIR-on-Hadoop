package driver;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import mapreduce.SequenceFileToImageMapper;
import mapreduce.SequenceFileToImagePartitioner;
import mapreduce.SequenceFileToImageReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SeqReader {
	// private static Log logger = LogFactory
	// .getLog(BinaryFilesToHadoopSequenceFile.class);

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		if (args.length < 3) {
			System.out.println("Usage: <jar file> <sequence filename(s)> "
					+ "<desired number of output sequence files> "
					+ "<ABSOLUTE path to hdfs locaiton where the output folder "
					+ "will automatically be created>");
			System.exit(0);
		}

		int numOutputFiles = Integer.parseInt(args[args.length - 2]);
		if (numOutputFiles < 1) {
			// someone is screwing around
			numOutputFiles = 1;
		}
		
		String absPath = args[args.length - 1];
		if (absPath.charAt(absPath.length() - 1) != '/') {
			absPath += "/";
		}

		DateFormat dateFormat = new SimpleDateFormat("ddMMyyyyHHmmss");
		Date date = new Date();
		String baseOutputName = absPath + "SeqReader" + dateFormat.format(date);

		Configuration conf = new Configuration();
		conf.set("BASE_OUTPUT_FILE_NAME", baseOutputName);
		conf.set("NUM_REDUCERS", Integer.toString(numOutputFiles));
		Job job = Job.getInstance(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SequenceFileToImageMapper.class);
		job.setReducerClass(SequenceFileToImageReducer.class);
		job.setPartitionerClass(SequenceFileToImagePartitioner.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(numOutputFiles);

		LazyOutputFormat.setOutputFormatClass(job,
				SequenceFileOutputFormat.class);

		for (int i = 0; i < args.length - 2; i++) {
			// FileInputFormat.setInputPaths(job, new Path(args[i]));
			MultipleInputs.addInputPath(job, new Path(args[i]),
					SequenceFileInputFormat.class);
		}
		for (int i = 0; i < numOutputFiles; i++) {
			MultipleOutputs.addNamedOutput(job, "n" + Integer.toString(i),
					SequenceFileOutputFormat.class, Text.class, Text.class);
		}
		job.setJarByClass(SeqReader.class);
		FileOutputFormat.setOutputPath(job, new Path(baseOutputName));
		
		/*	write the output folder location 
		 * 	to a file in the destination folder
		 */
		Path f = new Path(absPath + "SeqReader.outputlocation");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(f)) {
			// File already exists.
			// Delete the file before proceeding.
			fs.delete(f, true);
		}
		FSDataOutputStream os = fs.create(f);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		br.write(baseOutputName);
		br.close();
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}