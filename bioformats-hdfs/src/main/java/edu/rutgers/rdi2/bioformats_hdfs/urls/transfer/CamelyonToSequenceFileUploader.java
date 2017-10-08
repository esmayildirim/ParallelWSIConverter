package edu.rutgers.rdi2.bioformats_hdfs.urls.transfer;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;

import edu.rutgers.rdi2.bioformats_hdfs.mrjob.IntArrayWritable;
import edu.rutgers.rdi2.bioformats_hdfs.urls.GenericURL;
import edu.rutgers.rdi2.yarn_distributed_transfer.v2.TransferUtilities;

//import edu.rutgers.rdi2.bioformats_hdfs.urls.GenericURL;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.CamelyonTileReaderWithLabels;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSValue;
//import edu.rutgers.rdi2.yarn_distributed_transfer.v2.TransferUtilities;



public class CamelyonToSequenceFileUploader {

	public static void main(String[] args) throws Exception{
		
		
		GenericURL src = TransferUtilities.returnURLType(args[0]);
		src.setURL(args[0]);
    	GenericURL dest = TransferUtilities.returnURLType(args[1]);
    	dest.setURL(args[1]);
    	URLTransfer transferobject = new URLTransfer(src,dest);
    	transferobject.transfer();
    	
    	//Transfer label file - ONLY FOR TUMOR 
    	String labelURL = args[0].substring(0, args[0].lastIndexOf("/")) + "-mask/" + args[0].substring(args[0].lastIndexOf("/")+1,args[0].lastIndexOf(".")) + "_Mask.tif";
        System.out.println("labelURL" + labelURL);
        GenericURL srcLabel = TransferUtilities.returnURLType(labelURL);
    	srcLabel.setURL(labelURL);
    	URLTransfer transferLabelObject = new URLTransfer(srcLabel, dest);
    	transferLabelObject.transfer();
		
    	
    	
    	
    	//Transfer complete 
    	//upload to HDFS as a sequence file
    	String localSrcURL = args[1]+"/"+args[0].substring(args[0].lastIndexOf("/")+1);
    	String localLabelSrcURL = args[1]+ "/" +args[0].substring(args[0].lastIndexOf("/")+1,args[0].lastIndexOf(".")) + "_Mask.tif"; //ONLY FOR TUMOR
    	String hdfsDestURL = args[2]+"/"+args[0].substring(args[0].lastIndexOf("/")+1)+".seq";
    	
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(URI.create(hdfsDestURL), conf); 
    	Path path = new Path(hdfsDestURL);
    	System.out.println("about to create seq files "+hdfsDestURL);
    	
    	SequenceFile.Writer writer = null;
    	writer = SequenceFile.createWriter(fs, conf, path, new SVSKey().getClass(), new  IntArrayWritable().getClass(), 
    			SequenceFile.CompressionType.RECORD, new DefaultCodec());
    	
    	System.out.println("writer initialized");
    	int tileWidthArg = new Integer(args[3]).intValue();
    	int tileHeightArg = new Integer(args[4]).intValue();
    	int seriesArg = new Integer(args[5]).intValue();
   
    	CamelyonTileReaderWithLabels readerObject = new CamelyonTileReaderWithLabels(localSrcURL, localLabelSrcURL, seriesArg, tileWidthArg, tileHeightArg );//ONLY FOR TUMOR
    //	CamelyonTileReaderWithLabels readerObject = new CamelyonTileReaderWithLabels(localSrcURL, seriesArg, tileWidthArg, tileHeightArg );
    	System.out.println("Labelreader object created\n");
    	System.out.println("reader object constructed "+ args[1]+"/"+args[0].substring(args[0].lastIndexOf("/")+1));
		
    	SVSKey key;
		long tileId = 0;
		while(readerObject.getNextTileWritable()!=-1)
		{   
            System.out.println("BEFORE");
		int tempLabel = CamelyonTileReaderWithLabels.findTumorLabel(readerObject.getLabeltile()); //ONLY FOR TUMOR
        //    int tempLabel = CamelyonTileReaderWithLabels.findNormalLabel(readerObject.getTile());
            System.out.println("AFTER");

			//System.out.println("Adding "+tileId+" "+xcoord + " "+ ycoord);
			key = new SVSKey();
			key.svsFileName = new Text(hdfsDestURL);
			key.tileID = new LongWritable(tileId++);
			key.xcoord = new LongWritable(readerObject.getPrevXCoord());//because they are already incremented
			key.ycoord = new LongWritable(readerObject.getPrevYCoord());//because they are already incremented
			key.tileWidth = new IntWritable(readerObject.getTileWidth());
			key.tileHeight = new IntWritable(readerObject.getTileHeight());	
			key.label = new IntWritable(tempLabel);
			System.out.println("KEY VALUES SET");
			if(tempLabel != -1){
				//System.out.println("");
				writer.append(key,new IntArrayWritable(readerObject.getTile()));
				System.out.println("Adding "+tileId+" "+key.xcoord + " "+ key.ycoord+ " " + key.tileHeight+ " "  + key.tileWidth + " " + key.label+ " "+ readerObject.getWidth() + 
						" " +readerObject.getHeight());
				/*BufferedImage img=new BufferedImage(key.tileWidth.get(), key.tileHeight.get(), BufferedImage.TYPE_INT_ARGB);
				int [] intArray = new int[readerObject.getTile().length];
				int item = 0;
				for (Writable writable: readerObject.getTile()) {                 // iterate
					IntWritable intWritable = (IntWritable)writable;  // cast
					intArray[item++] = intWritable.get();                    // get
					// do your thing with int value
				}
				img.setRGB(0, 0, key.tileWidth.get(), key.tileHeight.get(), intArray, 0, key.tileWidth.get());
				ImageIO.write(img, "png", new File("/Users/eyildirim/Documents/output2/"+ Math.random() +"-"+key.tileID.get()+".png"));
				*/
			}


		}
		readerObject.close();
		IOUtils.closeStream(writer);
		/*
		byte []tile = null;
		IntWritable []nextTile = null;
		SVSKey key;
		SVSValue value;
		long tileId = 0;
		long xcoord = 0;
		long ycoord = 0;
		int tempTileWidth = tileWidthArg;
		int tempTileHeight = tileHeightArg;
		
		while((nextTile = readerObject.getNextTileWritable())!=null)
		{   
			
			boolean nextRow = false;
			boolean lasttile = false;
			
			IntWritable [] labelTile = readerObject.getNextLabelTile(); 
			int tempLabel = CamelyonTileReaderWithLabels.findTumorLabel(labelTile);
			
			
				
				key = new SVSKey();
				key.svsFileName = new Text( hdfsDestURL);
				key.tileID = new LongWritable(tileId++);
				key.xcoord = new LongWritable(xcoord);
				key.ycoord = new LongWritable(ycoord);
				if(xcoord + tempTileWidth >= readerObject.getWidth())	
				{	tempTileWidth = (int)(readerObject.getWidth() - xcoord);
				    nextRow = true;
				}
				key.tileWidth = new IntWritable(tempTileWidth);
				
				if(ycoord + tempTileHeight >= readerObject.getHeight())	
				{	
					tempTileHeight = (int) (readerObject.getHeight()- ycoord);
					lasttile = true;
				}
				key.tileHeight = new IntWritable(tempTileHeight);	
				key.label = new IntWritable(tempLabel);
				//value = new SVSValue();
				//value.RGBBytes = new BytesWritable(tile, tile.length);
				System.out.println("Adding "+tileId+" "+key.xcoord + " "+ key.ycoord+ " " + key.tileHeight+ " "  + key.tileWidth + " " + key.label+ " "+ readerObject.getWidth() + 
						" " +readerObject.getHeight());
				
				if(tempLabel != -1)
				{	
					writer.append(key,new IntArrayWritable(nextTile));
					BufferedImage img=new BufferedImage(key.tileWidth.get(), key.tileHeight.get(), BufferedImage.TYPE_INT_ARGB);
					int [] intArray = new int[nextTile.length];
					int item = 0;
					for (Writable writable: nextTile) {                 // iterate
						IntWritable intWritable = (IntWritable)writable;  // cast
						intArray[item++] = intWritable.get();                    // get
						// do your thing with int value
					}
					img.setRGB(0, 0, key.tileWidth.get(), key.tileHeight.get(), intArray, 0, key.tileWidth.get());
					ImageIO.write(img, "png", new File("/Users/eyildirim/Documents/output2/"+ Math.random() +"-"+key.tileID.get()+".png"));
				
				}
			//Create an SVS Key and Value object store them on a sequence file 
			if(nextRow)
			{	xcoord = 0;
			    ycoord += tempTileHeight;
			}
			else{
				xcoord +=tempTileWidth;
				
			}
			tempTileWidth = tileWidthArg;
			tempTileHeight = tileHeightArg;
			
		
		}
		readerObject.close();
		IOUtils.closeStream(writer);
    		
    	*/
	

	}

}
