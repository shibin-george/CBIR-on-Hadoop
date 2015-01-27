import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileToImageMapper extends
		Mapper<Object, Text, IntWritable, Text> {

	private static Log logger = LogFactory
			.getLog(SequenceFileToImageMapper.class);

	public void map(Object key, Text value, Context contex) throws IOException,
			InterruptedException {
		logger.info("map method called.. " + value.toString() + "\n");

		String uri = value.toString();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable k = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			BytesWritable v = (BytesWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			long position = reader.getPosition();
			Text t = new Text();
			IntWritable c = new IntWritable();
			while (reader.next(k, v)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				c.set((int) position);
				t.set(k.toString());
				contex.write(c, t);
				logger.info(position + " " + syncSeen + "\t" + k + "\t"
						+ reader.getValueClassName());
				position = reader.getPosition(); // beginning of next record
				BufferedImage b;
				byte[] data = v.copyBytes();
				// int h = getHeightFromKey(k.toString());
				// int w = getWidthFromKey(k.toString());

				InputStream is = new ByteArrayInputStream(data);
				// FSDataInputStream in = new FSDataInputStream(is);
				// in.seek(0);
				BufferedImage img = ImageIO.read(is);
				int h1 = img.getHeight();
				int w1 = img.getWidth();
				float Bpp = (float) data.length / (h1 * w1);
				// img.getType();
				logger.info("\n" + img.getType() + "\t" + data.length + "\t"
						+ Bpp + " BytesPerPixel" + "\t" + h1 + " X " + w1);

				GrayScaleFilter gsf = new GrayScaleFilter(img);

				// get the grayscale image using the filter
				BufferedImage grayImage = gsf.convertToGrayScale();

				// extract the feature from the grayscale image
				// using the LTrPFeatureExtractor
				LTrPFeatureExtractor lfe = new LTrPFeatureExtractor(grayImage);
				lfe.extractFeature();
				int[] featureVector = lfe.getFeatureVector();
				String s = "";
				for (int i = 0; i < featureVector.length; i++) {
					s += featureVector[i] + "\t";
					if((i+1)%59==0)
						s+="\n";
				}

				// BufferedImage biImage = gsf.convertToBiLevel();
				// byte[][] img = v.getBytes();
				// ImageInputStream in = ImageIO.createImageInputStream(v);
				// b = ImageIO.read(v);
				// String out = getfilename(k.toString());
				// conf = new Configuration();
				String out = getfilename(k.toString()) + ".feature";
				fs = FileSystem.get(conf);
				Path o = new Path(out);
				FSDataOutputStream os = fs.create(o);
				BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
				br.write(s);
				// ImageIO.write(biImage, "jpg", os);
				br.close();
				//os.close();
				
			}
		} finally {

		}
	}

	String getfilename(String k) {
		int p = k.indexOf(".jpg");
		return (k.substring(0, p));// + "bi.jpg");
	}

	int getHeightFromKey(String k) {
		int h = 0;
		int p1 = k.indexOf("@@");
		int p2 = k.indexOf("X", p1);
		String v = k.substring(p1 + 2, p2);
		h = Integer.parseInt(v);
		return h;
	}

	int getWidthFromKey(String k) {
		int w = 0;
		int p1 = k.indexOf("@@");
		int p2 = k.indexOf("X", p1);
		p1 = k.indexOf("@@", p2);
		String v = k.substring(p2 + 1, p1);
		w = Integer.parseInt(v);
		return w;
	}
}