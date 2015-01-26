import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SeqReader {
	//private static Log logger = LogFactory
	//		.getLog(BinaryFilesToHadoopSequenceFile.class);


	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		String uri = args[0];
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(URI.create(uri), conf);
//		Path path = new Path(uri);
//
//		SequenceFile.Reader reader = null;
//		try {
//			reader = new SequenceFile.Reader(fs, path, conf);
//			Writable key = (Writable) ReflectionUtils.newInstance(
//					reader.getKeyClass(), conf);
//			Writable value = (Writable) ReflectionUtils.newInstance(
//					reader.getValueClass(), conf);
//			long position = reader.getPosition();
//			while (reader.next(key, value)) {
//				String syncSeen = reader.syncSeen() ? "*" : "";
//				System.out.printf("[%s%s]\t%s\n", position, syncSeen, key);
//				position = reader.getPosition(); // beginning of next record
//			}
//		} finally {
//			System.exit(0);
//		}
		
		Job job = Job.getInstance(new Configuration());
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);

		job.setMapperClass(SequenceFileToImageMapper.class);
		//job.setReducerClass(SumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		for(int i=0;i<args.length - 1;i++){
			//FileInputFormat.setInputPaths(job, new Path(args[i]));
			MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class);
		}
		job.setJarByClass(SeqReader.class);
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		job.submit();
	}
}