package mapreduce;

import imagefeature.GrayScaleFilter;
import imagefeature.LTrPFeatureExtractor;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceFileToImageMapper extends
		Mapper<Text, BytesWritable, Text, Text> {

	private static int reducerNumber = 0, numReducers, count;
	private static Log logger = LogFactory
			.getLog(SequenceFileToImageMapper.class);

	protected void setup(Context context) {
		numReducers = Integer.parseInt(context.getConfiguration().get(
				"NUM_REDUCERS"));
		logger.info("Setup: num_reducers = " + numReducers);
		count = 0;
	}

	protected void cleanup(Context context) {
		logger.info("Cleanup: Total count received: " + count);
	}

	public void map(Text k, BytesWritable v, Context contex)
			throws IOException, InterruptedException {
		logger.info("map method called.. " + k.toString() + "\n");
		count++;
		logger.info("Total count received: " + count);
		Text t1 = new Text();
		Text t2 = new Text();

		String r = Integer.toString(reducerNumber % numReducers);
		reducerNumber++;
		t1.set(k.toString().split("_r_")[0] + "_r_" + r);

		byte[] data = v.copyBytes();
		InputStream is = new ByteArrayInputStream(data);
		BufferedImage img = ImageIO.read(is);
		int h1 = img.getHeight();
		int w1 = img.getWidth();
		float Bpp = (float) data.length / (h1 * w1);
		logger.info("\n" + img.getType() + "\t" + data.length + "\t" + Bpp
				+ " BytesPerPixel" + "\t" + h1 + " X " + w1);

		GrayScaleFilter gsf = new GrayScaleFilter(img);

		// get the grayscale image using the filter
		BufferedImage grayImage = gsf.convertToGrayScale();

		// extract the feature from the grayscale image
		// using the LTrPFeatureExtractor
		LTrPFeatureExtractor lfe = new LTrPFeatureExtractor(grayImage);
		lfe.extractFeature();
		double[] featureVector = lfe.getFeatureVector();
		String s = "";
		/*
		 * for (int i = 0; i < featureVector.length; i++) { s +=
		 * featureVector[i] + "\t"; if((i+1)%59==0) s+="\n"; }
		 * 
		 * // BufferedImage biImage = gsf.convertToBiLevel(); // byte[][] img =
		 * v.getBytes(); // ImageInputStream in =
		 * ImageIO.createImageInputStream(v); // b = ImageIO.read(v); // String
		 * out = getfilename(k.toString()); // conf = new Configuration();
		 * String out = getfilename(k.toString()) + ".feature"; fs =
		 * FileSystem.get(conf); Path o = new Path(out); FSDataOutputStream os =
		 * fs.create(o); BufferedWriter br = new BufferedWriter( new
		 * OutputStreamWriter( os, "UTF-8" ) ); br.write(s); //
		 * ImageIO.write(biImage, "jpg", os); br.close(); //os.close();
		 */
		for (int i = 0; i < featureVector.length; i++) {
			s += featureVector[i] + "_";
		}
		s += "42"; // appending a dummy value.
					// also, 42 is the answer to
					// Life, the Universe & Everything.

		t2.set(s);
		contex.write(t1, t2);
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