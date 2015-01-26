import java.awt.image.BufferedImage;
import java.awt.image.Raster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LTrPFeatureExtractor {
	private BufferedImage grayImage = null;
	private int width, height;
	private int image[][], fod[][], sod[][]; // fod stores the first order
												// derivative and sod, the
												// second order derivative
	private int uniPattern[] = { 0, 1, 2, 3, 4, 6, 7, 8, 12, 14, 15, 16, 24,
			28, 30, 31, 32, 48, 56, 60, 62, 63, 64, 96, 112, 120, 124, 126,
			127, 128, 129, 131, 135, 143, 159, 191, 192, 193, 195, 199, 207,
			223, 224, 225, 227, 231, 239, 240, 241, 243, 247, 248, 249, 251,
			252, 253, 254, 255 };

	private boolean isUniform[];
	private static Log logger = LogFactory.getLog(LTrPFeatureExtractor.class);

	private int histogram[][], index[];

	public LTrPFeatureExtractor(BufferedImage img) {
		grayImage = img;
		width = grayImage.getWidth();
		height = img.getHeight();
		byte[] pixel = null;
		image = new int[height + 2][width + 2];
		if (grayImage.getType() == BufferedImage.TYPE_BYTE_GRAY) {
			Raster raster = grayImage.getRaster();
			pixel = (byte[]) raster.getDataElements(0, 0, width, height, pixel);
			int l = pixel.length, k = 0;
			// String s = "";
			for (int i = 1; i <= height; i++) {
				for (int j = 1; j <= width; j++) {
					image[i][j] = pixel[k++] & 0xff;
					// s += image[i][j] + ".";
				}
				// s += "\n";
			}
			// logger.info(s);
		} else {
			logger.info("Image not of type grayscale");
		}

		// set up the uniform patterns
		isUniform = new boolean[256];
		int l = uniPattern.length;
		for (int i = 0; i < l; i++) {
			isUniform[i] = false;
		}
		for (int i = 0; i < l; i++) {
			isUniform[uniPattern[i]] = true;
		}

		// set up the histograms
		// There are 13 diiferent histograms, each of length 59.
		// Also initialize the indices of the uniform patterns
		histogram = new int[13][59];
		index = new int[256];
		for (int i = 0; i < l; i++) {
			index[uniPattern[i]] = i;
		}
		// 256 is the "miscellaneous" label for non-unniform patterns
		index[256] = 58;
	}

	public void fillBoundaryValues() {
		// shape up the image for LTrP feature extraction & CBIR
		int x;

		// fill the top row
		for (int i = 1; i <= width; i++) {
			x = 2 * image[1][i] - image[2][i];
			image[0][i] = (x > 0) ? x : 0;
			image[0][i] = (x < 255) ? x : 255;
		}

		// fill the bottom row
		for (int i = 1; i <= width; i++) {
			x = 2 * image[height][i] - image[height - 1][i];
			image[height + 1][i] = (x > 0) ? x : 0;
			image[height + 1][i] = (x < 255) ? x : 255;
		}

		// fill the first column
		for (int i = 1; i <= height; i++) {
			x = 2 * image[i][1] - image[i][2];
			image[i][0] = (x > 0) ? x : 0;
			image[i][0] = (x < 255) ? x : 255;
		}

		// fill the last column
		for (int i = 1; i <= height; i++) {
			x = 2 * image[i][width] - image[i][width - 1];
			image[i][width + 1] = (x > 0) ? x : 0;
			image[i][width + 1] = (x < 255) ? x : 255;
		}

		// fill the four corners of the image
		image[0][0] = (image[0][1] + image[1][0]) / 2;
		image[0][width + 1] = (image[0][width] + image[1][width + 1]) / 2;
		image[height + 1][0] = (image[height][0] + image[height + 1][1]) / 2;
		image[height + 1][width + 1] = (image[height][width + 1] + image[height + 1][width]) / 2;
	}

	public void calculateFirstOrderDerivative() {
		fod = new int[height + 1][width + 1];
		int h, v, c;

		/*
		 * The lines below compute first order derivative, which, frankly, is
		 * just a matter of some if-else conditions. But out of sheer
		 * joblessness, and the added pressure of always wanting to be a badass,
		 * I present to you... this. It was hard to write, it should be hard to
		 * read. -- Shibin George
		 */
		for (int i = 1; i <= height + 1; i++) {
			for (int j = 0; j <= width; j++) {
				h = image[i][j + 1] - image[i][j];
				h /= Math.abs(h);
				v = image[i - 1][j] - image[i][j];
				v /= Math.abs(v);
				if (h > v) {
					c = 4;
				} else if (h < v) {
					c = 2;
				} else {
					if (h > 0) {
						c = 1;
					} else {
						c = 3;
					}
				}
				fod[i][j] = c;
			}
		}

		// fill the top row
		for (int i = 0; i <= width; i++) {
			fod[0][i] = (image[0][i + 1] > image[0][i]) ? 1 : 2;
		}
		fod[0][width + 1] = 1;

		// fill the last column
		for (int i = 1; i <= height + 1; i++) {
			fod[i][width + 1] = (image[i - 1][width + 1] > image[i][width + 1]) ? 1
					: 4;
		}
	}

	public void calculateTetraPatterns() {
		int[] tPattern = new int[8];
		int d, pattern[] = new int[3];

		for (int i = 1; i <= height; i++) {
			for (int j = 1; j <= width; j++) {
				d = fod[i][j];
				tPattern[0] = (fod[i][j] == fod[i][j + 1]) ? 0 : fod[i][j + 1];
				tPattern[1] = (fod[i][j] == fod[i - 1][j + 1]) ? 0
						: fod[i - 1][j + 1];
				tPattern[2] = (fod[i][j] == fod[i - 1][j]) ? 0 : fod[i - 1][j];
				tPattern[3] = (fod[i][j] == fod[i - 1][j - 1]) ? 0
						: fod[i - 1][j - 1];
				tPattern[4] = (fod[i][j] == fod[i][j - 1]) ? 0 : fod[i][j - 1];
				tPattern[5] = (fod[i][j] == fod[i + 1][j - 1]) ? 0
						: fod[i + 1][j - 1];
				tPattern[6] = (fod[i][j] == fod[i + 1][j]) ? 0 : fod[i + 1][j];
				tPattern[7] = (fod[i][j] == fod[i + 1][j + 1]) ? 0
						: fod[i + 1][j + 1];

				pattern[0] = pattern[1] = pattern[2] = 0;

				int c, f;
				for (int k = 0; k <= 7; k++) {
					c = 0;
					for (int l = 1; l <= 4; l++) {
						if (l != d) {
							f = ((tPattern[k] ^ l) == 0) ? 1 : 0;
							pattern[c] = ((pattern[c] << k) | f) & 0x000000ff;
						}
					}
				}
				// add to the histogram
				for (int t = 0; t < 2; t++) {
					if (isUniform[pattern[t]]) {
						histogram[(d - 1) * 3 + t][index[pattern[t]]] += 1;
					} else {
						// non-uniform patterns are grouped under "256"
						histogram[(d - 1) * 3 + t][58] += 1;
					}
				}
			}
		}
	}

	public void extractFeature() {
		fillBoundaryValues();
		calculateFirstOrderDerivative();
		calculateTetraPatterns();

		// At this stage, we have the histogram set up with all the
		// uniform-patterns.
		// Only the features remain to be extracted
	}
}
