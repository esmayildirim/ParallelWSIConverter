package edu.rutgers.rdi2.yarn_distributed_transfer.svs;

import java.awt.FlowLayout;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.openslide.OpenSlide;

import edu.rutgers.rdi2.bioformats_hdfs.mrjob.IntArrayWritable;
import edu.rutgers.rdi2.bioformats_hdfs.urls.GenericURL;
import edu.rutgers.rdi2.bioformats_hdfs.urls.transfer.URLTransfer;
import edu.rutgers.rdi2.yarn_distributed_transfer.v2.TransferUtilities;
/*
 * A special class for reading Camelyon Dataset Tiles with their corresponding
 * class labels 
 * Uses Openslide library to understand the WSI image file format in the data set
 */

public class CamelyonTileReaderWithLabels {
	OpenSlide reader;
	OpenSlide labelReader;
	long xCoord;
	long yCoord;
	long prevXCoord;
	
	long prevYCoord;
	int planeId;
	int tileWidth;
	int tileHeight;
	long width;
	long height;
	int label;
	IntWritable[] tile;
	IntWritable[] labeltile;
	
	public long getPrevXCoord() {
		return prevXCoord;
	}
	public void setPrevXCoord(long prevXCoord) {
		this.prevXCoord = prevXCoord;
	}
	public long getPrevYCoord() {
		return prevYCoord;
	}
	public void setPrevYCoord(long prevYCoord) {
		this.prevYCoord = prevYCoord;
	}
	public IntWritable[] getTile() {
		return tile;
	}
	public void setTile(IntWritable[] tile) {
		this.tile = tile;
	}
	public IntWritable[] getLabeltile() {
		return labeltile;
	}
	public void setLabeltile(IntWritable[] labeltile) {
		this.labeltile = labeltile;
	}
	
	public OpenSlide getLabelReader() {
		return labelReader;
	}
	public void setLabelReader(OpenSlide labelReader) {
		this.labelReader = labelReader;
	}
	public int getLabel() {
		return label;
	}
	public void setLabel(int label) {
		this.label = label;
	}
	public long getxCoord() {
		return xCoord;
	}
	public void setxCoord(int xCoord) {
		this.xCoord = xCoord;
	}
	public long getyCoord() {
		return yCoord;
	}
	public void setyCoord(int yCoord) {
		this.yCoord = yCoord;
	}
	public long getWidth() {
		return width;
	}
	public void setWidth(int width) {
		this.width = width;
	}
	public long getHeight() {
		return height;
	}
	public void setHeight(int height) {
		this.height = height;
	}

	public int getPlaneId() {
		return planeId;
	}
	public void setPlaneId(int planeId) {
		this.planeId = planeId;
	}
	public int getTileWidth() {
		return tileWidth;
	}
	public void setTileWidth(int tileWidth) {
		this.tileWidth = tileWidth;
	}
	public int getTileHeight() {
		return tileHeight;
	}
	public void setTileHeight(int tileHeight) {
		this.tileHeight = tileHeight;
	}

	
	public CamelyonTileReaderWithLabels(String localURL, String localLabelURL, int plane, int tw, int th) throws IOException
	{
		reader = new OpenSlide(new File(localURL));
		labelReader = new OpenSlide(new File(localLabelURL));
	    width = reader.getLevelWidth(plane);
	    height = reader.getLevelHeight(plane);
		xCoord = 0;
		yCoord = 0;
		planeId = plane;
		tileHeight = th;
		tileWidth = tw;
	}
	public CamelyonTileReaderWithLabels(String localURL, int plane, int tw, int th) throws IOException
	{
		reader = new OpenSlide(new File(localURL));
		xCoord = 0;
		yCoord = 0;
		planeId = plane;
		width = reader.getLevelWidth(planeId);
		height = reader.getLevelHeight(planeId);
		tileHeight = th;
		tileWidth = tw; 
	}
	
/*	
	public byte[] readTile(long xCoord, long yCoord, int tileWidth, int tileHeight) throws IOException
	{
		
		int dest[] = new int [tileWidth * tileHeight];
		System.out.println("READING: "+xCoord+" "+yCoord+" "+planeId+" "+tileWidth+ " "+ tileHeight);
	
		reader.paintRegionARGB(dest, xCoord, yCoord,  planeId, tileWidth, tileHeight);
		
		
		byte[] tile = new byte[dest.length * 4];

        for (int i = 0, j = 0; i < dest.length; i++, j+=4) {
            tile[j] = (byte) ((dest[i] & 0xFF000000) >> 24);
            tile[j+1] = (byte) ((dest[i] & 0x00FF0000) >> 16);
            tile[j+2] = (byte) ((dest[i] & 0x0000FF00) >> 8);
            tile[j+3] = (byte) ((dest[i] & 0x000000FF) >> 0);
            //System.out.println((tile[j] & 0xFF) +" "+(tile[j+1] & 0xFF) +" "+(tile[j+2] & 0xFF) + " "+(tile[j+3] & 0xFF) );
        }
		return tile;
	}
*/	
	public IntWritable[] readTileWritable() throws IOException
	{
		
		int dest[] = new int [this.tileWidth * this.tileHeight];
		System.out.println("READING: "+this.xCoord+" "+this.yCoord+" "+this.planeId+" "+this.tileWidth+ " "+ this.tileHeight);
		reader.paintRegionARGB(dest, this.xCoord, this.yCoord,  this.planeId, this.tileWidth, this.tileHeight);
		
		
		IntWritable[] tile = new IntWritable[this.tileWidth * this.tileHeight];

        for (int i = 0; i < dest.length; i++) {
        	tile[i] = new IntWritable(dest[i]);
            //System.out.println((tile[j] & 0xFF) +" "+(tile[j+1] & 0xFF) +" "+(tile[j+2] & 0xFF) + " "+(tile[j+3] & 0xFF) );
        }
		return tile;
	}
	
	
	public IntWritable[] readLabelTile() throws IOException
	{
		int dest[] = new int [this.tileWidth * this.tileHeight];
		System.out.println("READING: "+this.xCoord+" "+this.yCoord+" "+this.planeId+" "+this.tileWidth+ " "+ this.tileHeight);
		labelReader.paintRegionARGB(dest, this.xCoord, this.yCoord,  this.planeId, this.tileWidth, this.tileHeight);
		
		
		IntWritable[] tile = new IntWritable[this.tileWidth * this.tileHeight];

        for (int i = 0; i < dest.length; i++) {
        	tile[i] = new IntWritable(dest[i]);
            //System.out.println((tile[j] & 0xFF) +" "+(tile[j+1] & 0xFF) +" "+(tile[j+2] & 0xFF) + " "+(tile[j+3] & 0xFF) );
        }
		return tile;
	}
	
	public int getNextTileWritable() throws IOException
	{
		boolean nextRow = false;
		boolean lastRow = false;
		
		if(this.xCoord >= this.getWidth())
		    nextRow = true;
		
		if(this.yCoord >= this.getHeight())
		    lastRow = true;

		//else temp_height = tileHeight;
		if(nextRow && lastRow)
		    return -1;
		else if(nextRow)
		{
		///	this.prevXCoord = this.xCoord - this.tileWidth;
			this.xCoord = 0;
			this.prevYCoord = this.yCoord;
		    this.yCoord += this.tileHeight;
		    
		}
		if(this.yCoord < this.getHeight())
		{
			this.tile = readTileWritable();
		    this.labeltile = readLabelTile();
			this.prevXCoord = this.xCoord;
			this.xCoord += this.tileWidth;
		}
		else return -1; 
		return 0;
	}

	public void close()throws IOException
	{
		reader.close();
		labelReader.close();
	}
	
	public static int findTumorLabel(IntWritable [] ints)
	{
		long count = 0;
		for(int i = 0; i < ints.length; i++){
			
			if(ints[i].get() == 0xFFFFFFFF) count++;
			   
		}
		//System.out.println("count:"+count);
		if(count > 0.8 * ints.length) return 1;
		else return -1;
	}
	public static int findNormalLabel(IntWritable [] ints)
	{
		long count = 0;
		int threshold = 180;
		for(int i = 0; i < ints.length ; i++ )
		{
			int  r,g, b; 
			//a = ints[i].get() & 0xFF000000 >> 24;
		    r = (ints[i].get() & 0x00FF0000) >> 16;
		    g = (ints[i].get() & 0x0000FF00) >> 8;
		    b = (ints[i].get() & 0x000000FF) >> 0;
			int grayPixel = (r + g + b)/3;
			if(grayPixel < threshold)
				count ++;
		}	
		if(count > 0.8 * ints.length) return 0;
		else return -1;
	}
	public static void main(String args[]) throws IOException
	{
	
		CamelyonTileReaderWithLabels readerObject = new CamelyonTileReaderWithLabels(
				"/Users/eyildirim/Documents/CNN_dspaces/camelyon-train-tumor/Tumor_009.tif",
				"/Users/eyildirim/Documents/CNN_dspaces/camelyon-train-tumor-mask/Tumor_009_Mask.tif", 0, 1024, 1024);
		
		SVSKey key;
		long tileId = 0;
		while(readerObject.getNextTileWritable()!=-1)
		{   
            System.out.println("BEFORE ");
			int tempLabel = CamelyonTileReaderWithLabels.findTumorLabel(readerObject.getLabeltile());
            System.out.println("AFTER ");

			if(tempLabel != -1)
			{
				//System.out.println("Adding "+tileId+" "+xcoord + " "+ ycoord);
				key = new SVSKey();
				key.svsFileName = new Text( "/Users/eyildirim/Documents/CNN_dspaces/camelyon-train-tumor/Tumor_009.tif");
				key.tileID = new LongWritable(tileId++);
				key.xcoord = new LongWritable(readerObject.getPrevXCoord());//because they are already incremented
				key.ycoord = new LongWritable(readerObject.getPrevYCoord());//because they are already incremented
				key.tileWidth = new IntWritable(readerObject.getTileWidth());
				key.tileHeight = new IntWritable(readerObject.getTileHeight());	
				key.label = new IntWritable(tempLabel);
				//writer.append(key,new IntArrayWritable(readerObject.getTile()));
				System.out.println("Adding "+tileId+" "+key.xcoord + " "+ key.ycoord+ " " + key.tileHeight+ " "  + key.tileWidth + " " + key.label+ " "+ readerObject.getWidth() + 
						" " +readerObject.getHeight());
				
			}
						
		}
		readerObject.close();
	
	}
	
}
