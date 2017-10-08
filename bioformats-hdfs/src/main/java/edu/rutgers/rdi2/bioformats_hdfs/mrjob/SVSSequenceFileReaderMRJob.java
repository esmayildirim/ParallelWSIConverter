package edu.rutgers.rdi2.bioformats_hdfs.mrjob;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;

import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSValue;
import edu.rutgers.rdi2.yarn_distributed_transfer.v2.ApplicationMaster;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatWriter;
import loci.formats.ImageWriter;
import loci.formats.MetadataTools;
import loci.formats.meta.IMetadata;

import loci.formats.services.OMEXMLService;

public class SVSSequenceFileReaderMRJob {
	private static final Log LOG = LogFactory.getLog(SVSSequenceFileReaderMRJob.class);
	public static class MyMapper extends
            Mapper<SVSKey, SVSValue, Text, LongWritable> {
		public static Logger logger =
                Logger.getLogger(MyMapper.class);
        public void map(SVSKey key, SVSValue value, Context context)
                throws IOException, InterruptedException {
        	
            String w = key.svsFileName +"-"+ key.tileID+"-" + key.xcoord +"-" + key.ycoord+"-"+key.tileWidth +"-" + key.tileHeight;
            
            byte []byteArray = new byte[value.RGBBytes.getLength()];
           
            System.arraycopy(value.RGBBytes.getBytes(), 0, byteArray, 0, value.RGBBytes.getLength());
           
            long calculatedSize = key.tileWidth.get()*key.tileHeight.get()*4;
            System.out.println(calculatedSize+"="+byteArray.length);
            //int pixelType = FormatTools.UINT8;
            int c = 4;
          
          //  IFormatWriter bioformatsWriter = new ImageWriter();
            String filePrefix = "/Users/eyildirim/Documents/output2/";
        /*   try{
           ServiceFactory factory = new ServiceFactory();
            OMEXMLService service = factory.getInstance(OMEXMLService.class);
            IMetadata meta = service.createOMEXMLMetadata();
            
            MetadataTools.populateMetadata(meta, 0, null, false, "XYZCT",
        		   FormatTools.getPixelTypeString(pixelType), (int)key.tileWidth.get(), (int)key.tileHeight.get(),1,c,1, c);
   
           	String filename = key.svsFileName.toString().substring(key.svsFileName.toString().lastIndexOf("/")+1);
           	bioformatsWriter.setMetadataRetrieve(meta);
       
            bioformatsWriter.setId(filePrefix + filename+"-"+key.tileID.toString() + "-" + 
		    		  "-" + key.xcoord.toString() + "-"+ key.ycoord.toString() +
		    		  "-" + key.tileWidth.toString() + "-" +key.tileHeight.toString()+
		    		  ".tiff");
            bioformatsWriter.saveBytes(0, byteArray);
            bioformatsWriter.close();
            
            */
            BufferedImage img=new BufferedImage(key.tileWidth.get(), key.tileHeight.get(), BufferedImage.TYPE_INT_ARGB);
       	 
            //convert byte array to int
            int [] intArray = new int[byteArray.length / 4];
            int j =0;
            for(int i = 0; i < byteArray.length; i+=4, j++)
            {
            	intArray[j] = (byteArray[i] & 0xFF ) << 24 | (byteArray[i+1] & 0xFF) << 16 | (byteArray[i+2] & 0xFF) << 8 | (byteArray[i +3] & 0xFF);
                LOG.info((byteArray[i] & 0xFF) +" "+ (byteArray[i+1] & 0xFF) +" "+ (byteArray[i+2] & 0xFF)+ " " + (byteArray[i+3] & 0xFF));
            }
            System.out.println(key.tileHeight.get()+" "+key.tileWidth.get());
            
       	    for(int i = 0; i < key.tileHeight.get(); i++)
       	    	for(int k = 0; k < key.tileWidth.get(); k++)
       	    	{
       	    		//System.out.println("x+" + k + " y="+ i + " value= "+ intArray[i * key.tileWidth.get() + k]);
       	    		img.setRGB(k,i, intArray[i * key.tileWidth.get() + k]);
       	    		
       	    	}
            
            
           // img.setRGB(0, 0, key.tileWidth.get(), key.tileHeight.get(), intArray, 0, key.tileWidth.get());
       		//ImageIO.write(img, "png", new File("/Users/eyildirim/Documents/output2/"+ Math.random() +"-"+key.tileID.get()+".png"));
   
      /*  }catch (DependencyException exc) {
			bioformatsWriter.close();
			//  throw new FormatException("Could not create OME-XML store.", exc);
			  
			}
			catch (ServiceException exc) {
				bioformatsWriter.close();
			//  throw new FormatException("Could not create OME-XML store.", exc);
			}
           catch (FormatException exc) {
				bioformatsWriter.close();
			//  throw new FormatException("Could not create OME-XML store.", exc);
			}
		
          */
            context.write(new Text(w+" "+ key.tileHeight.get()+" "+key.tileWidth.get()), new LongWritable(byteArray.length/ 4));
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
    	
    	Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SVSSequenceFileReaderMRJob.class);
        job.setMapperClass(MyMapper.class);
       
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);
  //      job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true);
        
        long end = System.currentTimeMillis();
        System.out.println(end-start+"\t msecs");
        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}