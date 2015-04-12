package imagefeature;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;

public class GrayScaleFilter {
	/*
	 * This class provides functionalities to manipulate gray-scale images
	 * including conversion of color-images to grayscale-images
	 */
	private BufferedImage colorImage = null;
	private BufferedImage grayImage = null;
	int width, height;

	public GrayScaleFilter(BufferedImage I) {
		this.colorImage = I;
		width = colorImage.getWidth();
		height = colorImage.getHeight();
	}

	public void setGrayImage(BufferedImage I) {
		this.grayImage = I;
		width = I.getWidth();
		height = I.getHeight();
	}

	public BufferedImage convertToGrayScale() {
		grayImage = new BufferedImage(width, height,
				BufferedImage.TYPE_BYTE_GRAY);
		WritableRaster raster = grayImage.getRaster();

		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				int rgb = colorImage.getRGB(i, j);
				int r = (rgb >> 16) & 0xff;
				int g = (rgb >> 8) & 0xff;
				int b = (rgb) & 0xff;

				byte gray = (byte) (.299 * r + .587 * g + .114 * b);
				raster.setSample(i, j, 0, gray);
			}
		}
		return grayImage;
	}

	public BufferedImage convertToBiLevel() {
		if (this.grayImage == null) {
			System.out.println("Please set the GrayImage for this filter");
			return null;
		}
		BufferedImage BWImage = new BufferedImage(width, height,
				BufferedImage.TYPE_BYTE_BINARY);
		WritableRaster raster = BWImage.getRaster();
		WritableRaster ras = grayImage.getRaster();

		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				int gray = ras.getSample(i, j, 0);
				if (gray > 0x7f) {
					raster.setSample(i, j, 0, 1);
				} else {
					gray = 0;
					raster.setSample(i, j, 0, 0);
				}
			}
		}
		return BWImage;
	}
	
}
