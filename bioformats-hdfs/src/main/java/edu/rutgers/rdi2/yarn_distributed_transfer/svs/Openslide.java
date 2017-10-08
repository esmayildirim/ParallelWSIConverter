package edu.rutgers.rdi2.yarn_distributed_transfer.svs;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import javax.imageio.ImageIO;

import org.openslide.OpenSlide;

public class Openslide {
    static void print_downsamples(OpenSlide osr) {
        for (int level = 0; level < osr.getLevelCount(); level++) {
            System.out.printf("level %d: downsample: %g\n", level, osr
                    .getLevelDownsample(level));
        }
    }

    static void test_next_biggest(OpenSlide osr, double downsample) {
        int level = osr.getBestLevelForDownsample(downsample);
        System.out.printf("level for downsample %g: %d (%g)\n", downsample,
                level, osr.getLevelDownsample(level));
    }

    public static void main(String args[]) throws IOException {
        //if (args.length != 1) {
      //      System.out.printf("give file!\n");
      //      return;
      //  }

        System.out.printf("version: %s\n", OpenSlide.getLibraryVersion());

        File f = new File("/Users/eyildirim/Documents/CNN_dspaces/camelyon-train-tumor/Tumor_009.tif");
        File labelf = new File("/Users/eyildirim/Documents/CNN_dspaces/camelyon-train-tumor-mask/Tumor_009_Mask.tif");
        System.out.printf("openslide_detect_vendor returns %s\n",
                OpenSlide.detectVendor(f));
        OpenSlide osr = new OpenSlide(f);
        OpenSlide osrlabel = new OpenSlide(labelf);
        long w, h;

        w = osr.getLevel0Width();
        h = osr.getLevel0Height();
        System.out.printf("dimensions: %d x %d\n", w, h);
        for(int i = 0 ; i < h ; i+=1024)
        	for(int j = 0; j < w ; j+=1024)
        	{
        		int []dest = new int[1024 * 1024];
                osrlabel.paintRegionARGB(dest, j, i, 0, 1024, 1024);
        		long count = 0; 
        		
        		for(int k = 0; k < dest.length; k++){
        			
        			if(dest[k] == 0xFFFFFFFF) count++;
        			   
        		}
        		//System.out.println("count:"+count);
        		if(count > 0.8 * dest.length)
        		{   int []dest2 = new int[1024 * 1024];
        			osr.paintRegionARGB(dest2, j, i, 0, 1024, 1024);
        			
        			BufferedImage img=new BufferedImage(1024, 1024, BufferedImage.TYPE_INT_ARGB);

        			img.setRGB(0, 0, 1024, 1024, dest2, 0, 1024);
        			ImageIO.write(img, "png", new File("/Users/eyildirim/Documents/output2/"+ Math.random() +".png"));
        			
        		}
        	}
       
        osr.dispose();
    }
}