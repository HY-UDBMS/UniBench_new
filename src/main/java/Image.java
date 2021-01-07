import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * Created by chzhang on 28/04/2017.
 */
public class Image {
    public static void loadImageFromFile() {
        try {
            BufferedImage image = ImageIO.read(new File("src/main/resources/image/image_0714.jpg"));
            Color c = new Color(image.getRGB(0, 0));
            int red = c.getRed();
            int green = c.getGreen();
            int blue = c.getBlue();

            System.out.println(red + green + blue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
