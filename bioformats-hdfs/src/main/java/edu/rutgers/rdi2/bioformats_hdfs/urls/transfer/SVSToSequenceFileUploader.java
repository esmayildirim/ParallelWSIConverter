package edu.rutgers.rdi2.bioformats_hdfs.urls.transfer;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;

import edu.rutgers.rdi2.bioformats_hdfs.urls.GenericURL;
//import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSTileReader;
//import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSValue;
import edu.rutgers.rdi2.yarn_distributed_transfer.v2.TransferUtilities;
//import edu.rutgers.rdi2.*;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSValue;
public class SVSToSequenceFileUploader {
	
	@SuppressWarnings("deprecation")
	public static void main(String args[])throws Exception
	{
		GenericURL src = TransferUtilities.returnURLType(args[0]);
		src.setURL(args[0]);
		//String destt = new String("/Users/eyildirim/Documents/tmp");
    	GenericURL dest = TransferUtilities.returnURLType(args[1]);
    	dest.setURL(args[1]);
    	URLTransfer transferobject = new URLTransfer(src,dest);
    	transferobject.transfer();

		//Transfer complete 
    	//upload to HDFS as a sequence file
    	String localSrcURL = args[1]+"/"+args[0].substring(args[0].lastIndexOf("/")+1);
    	String hdfsDestURL = args[2]+"/"+args[0].substring(args[0].lastIndexOf("/")+1)+".seq";
    	
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(URI.create(hdfsDestURL), conf); 
    	Path path = new Path(hdfsDestURL);
    	
    	SequenceFile.Writer writer = null;
    	writer = SequenceFile.createWriter(fs, conf, path, new SVSKey().getClass(), new SVSValue().getClass(), 
    			SequenceFile.CompressionType.RECORD, new BZip2Codec());
    //	writer = SequenceFile.createWriter(fs, conf, path, new SVSKey().getClass(), new SVSValue().getClass());
    	
    	
    	
    	SVSTileReader readerObject = new SVSTileReader(localSrcURL);
    	System.out.println(args[1]+"/"+args[0].substring(args[0].lastIndexOf("/")+1));
		
    	int tileWidthArg = new Integer(args[3]).intValue();
    	int tileHeightArg = new Integer(args[4]).intValue();
    	int seriesArg = new Integer(args[5]).intValue();
    	readerObject.setTileWidth(tileWidthArg);
		readerObject.setTileHeight(tileHeightArg);
		readerObject.setSeries(seriesArg);
		System.out.println("Series number"+seriesArg);
		byte []tile = null;
		SVSKey key;
		SVSValue value;
		long tileId = 0;
		long xcoord = 0;
		long ycoord = 0;
		int tileWidth = tileWidthArg;
		int tileHeight = tileHeightArg;
		while((tile = readerObject.getNextTile())!=null)
		{   
			boolean nextRow = false;
			boolean lasttile = false;
			//Create an SVS Key and Value object store them on a sequence file 
			key = new SVSKey();
			key.svsFileName = new Text(hdfsDestURL);
			key.tileID = new LongWritable(tileId++);
			key.xcoord = new LongWritable(xcoord);
			key.ycoord = new LongWritable(ycoord);
			if(xcoord + tileWidth >= readerObject.getWidth())
			{	tileWidth = (int)(readerObject.getWidth() - xcoord);
			    nextRow = true;
			}
			
			key.tileWidth = new IntWritable(tileWidth);
			if(ycoord + tileHeight >= readerObject.getHeight())
			{	tileHeight = (int)(readerObject.getHeight()- ycoord);
			    lasttile = true;
			}
			key.tileHeight = new IntWritable(tileHeight);	
			
			value = new SVSValue();
			value.RGBBytes = new BytesWritable(tile, tile.length);
			
			writer.append(key, value);
			
			if(nextRow)
			{	xcoord = 0;
			    ycoord += tileHeight;
			}
			else{
				xcoord +=tileWidth;
				
			}
			tileWidth = tileWidthArg;
			tileHeight = tileHeightArg;
		}
		readerObject.close();
		IOUtils.closeStream(writer);
    	
    	
    			
    	
	}

}
