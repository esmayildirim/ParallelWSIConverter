package edu.rutgers.rdi2.bioformats_hdfs;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ome.xml.meta.IMetadata;
import ome.xml.model.primitives.PositiveInteger;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.IFormatReader;
import loci.formats.IFormatWriter;
import loci.formats.ImageReader;
import loci.formats.ImageWriter;
import loci.formats.MetadataTools;
import loci.formats.services.OMEXMLService;
import loci.formats.meta.MetadataStore;


public class IntegrationMRJob {
	public static class MyMapper extends
    Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    String w = value.toString();
    context.write(new Text(w), new Text(w));
} }
public static class MyReducer extends
    Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
    int sum = 0;
    String path = key.toString();
//    for (IntWritable val : values) {
//        sum += val.get();
//    }
   context.write(key, key);
    IFormatReader reader = new ImageReader();
	// tell the reader where to store the metadata from the dataset
	MetadataStore metadata;
	OMEXMLService service;
	ServiceFactory factory;
	try {
	  factory = new ServiceFactory();
	  service = factory.getInstance(OMEXMLService.class);
	  metadata = service.createOMEXMLMetadata();
	}
	catch (DependencyException exc) {
		reader.close();
	  throw new IOException("Could not create OME-XML store.", exc);
	  
	}
	catch (ServiceException exc) {
		reader.close();
	  throw new IOException("Could not create OME-XML store.", exc);
	}
	
	reader.setMetadataStore(metadata);
	// initialize the dataset
	try{
		reader.setId(path);	
	}
	catch(FormatException exc)
	{
		reader.close();
		  throw new IOException("Could not create OME-XML store.", exc);
		
		
	}
	
	int tileWidth = 1024;
	int tileHeight = 1024;

     int series = 2;
     reader.setSeries(series);
	  

	  // determine how many tiles are in each image plane
	  // for simplicity, we'll assume that the image width and height are
	  // multiples of 1024

	  int tileRows = reader.getSizeY() / tileHeight;
	  int tileColumns = reader.getSizeX() / tileWidth;
	  for (int image=0; image<reader.getImageCount(); image++) {
         System.out.println("Series:"+series+", Planes:"+reader.getImageCount()+", SizeX:"+reader.getSizeX()+", SizeY:"+reader.getSizeY());

	    for (int row=0; row<tileRows; row++) {
	      for (int col=0; col<tileColumns; col++) {
	        // open a tile - in addition to the image index, we need to specify
	        // the (x, y) coordinate of the upper left corner of the tile,
	        // along with the width and height of the tile
	        int xCoordinate = col * tileWidth;
	        int yCoordinate = row * tileHeight;
	        
	    //    boolean lastTileWidth = false;
	    //    boolean lastTileHeight = false;
	        byte[] tile = null;

	      
	          ImageWriter writer = new ImageWriter();
	          String filePrefix = "/Users/eyildirim/Documents/output/TCGA-output-";
               MetadataStore store = reader.getMetadataStore();
               store.setPixelsSizeX(new PositiveInteger(tileWidth), 0);
               store.setPixelsSizeY(new PositiveInteger(tileHeight), 0);
		          writer.setMetadataRetrieve(service.asRetrieve(store));
		       try{
		          writer.setId(filePrefix + series + "-" + image + "-" + row + "-" + col + ".jpeg");

	     
	            writer.saveBytes(image, reader.openBytes(image, xCoordinate, yCoordinate, tileWidth, tileHeight));
	      }
	      catch(FormatException exc)
	      {
	    	  writer.close();
	    	  throw new IOException("Format Exception",exc);
	    	  
	    	  
	      }
	            // }
	          writer.close();
	        //}
	        
	        
	        
	        
	       // }
	      }
	        	
	      }
	    }
}
}
public static void main(String[] args) throws Exception {
Job job = Job.getInstance(new Configuration());
job.setJarByClass(IntegrationMRJob.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setMapperClass(MyMapper.class);
job.setReducerClass(MyReducer.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
job.setNumReduceTasks(2);
FileInputFormat.setInputPaths(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
boolean status = job.waitForCompletion(true);
if (status) {
    System.exit(0);
} else {
    System.exit(1);
} }
}
