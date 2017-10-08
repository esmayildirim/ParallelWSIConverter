package edu.rutgers.rdi2.yarn_distributed_transfer.svs;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.meta.MetadataStore;
import loci.formats.services.OMEXMLService;

public class SVSTileReader {
	IFormatReader reader;
	MetadataStore metadata;
	OMEXMLService service;
	ServiceFactory factory;
	
	int xCoord;
	int yCoord;
	int planeId;
	int tileWidth;
	int tileHeight;
	int width;
	int height;
	public int getxCoord() {
		return xCoord;
	}
	public void setxCoord(int xCoord) {
		this.xCoord = xCoord;
	}
	public int getyCoord() {
		return yCoord;
	}
	public void setyCoord(int yCoord) {
		this.yCoord = yCoord;
	}
	public int getWidth() {
		return width;
	}
	public void setWidth(int width) {
		this.width = width;
	}
	public int getHeight() {
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

	
	
	public SVSTileReader() throws FormatException, IOException
	{
		reader = new ImageReader();
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
		  throw new FormatException("Could not create OME-XML store.", exc);
		  
		}
		catch (ServiceException exc) {
			reader.close();
		  throw new FormatException("Could not create OME-XML store.", exc);
		}
		reader.setMetadataStore(metadata);
		xCoord = 0;
		yCoord = 0;
		planeId = 0;
	}
	public SVSTileReader(String localURL) throws FormatException, IOException
	{
		this();
		reader.setId(localURL);
		
	}
	public void setImageURL(String localURL) throws FormatException, IOException
	{
		reader.setId(localURL);
		
	}
	public void setSeries(int series)
	{
		reader.setSeries(series);
		width = reader.getSizeX();
		System.out.println("WIDTH:"+width);
		height = reader.getSizeY();
		System.out.println("HEIGHT"+height);
	}
	
	public byte[] readTile(int xCoord, int yCoord, int tileWidth, int tileHeight, int planeId) throws FormatException, IOException
	{
		byte tile[] = reader.openBytes(planeId, xCoord, yCoord, tileWidth, tileHeight); 
		return tile;
	}
	
	public byte[] getNextTile() throws FormatException, IOException
	{
		byte tile[] = null;
		boolean nextRow = false;
		boolean lastRow = false;
		if(this.xCoord == width && this.yCoord == height )
			return null;
		int temp_width, temp_height;
		
		if(this.xCoord + this.tileWidth >= this.getWidth())
		{	
			temp_width =  this.getWidth() - this.xCoord ;
		    nextRow = true;
		}else temp_width = tileWidth;
		
		if(this.yCoord + this.tileHeight >= this.getHeight())
		{	temp_height = this.getHeight() -  this.yCoord ;
		    lastRow = true;
		}
		else temp_height = tileHeight;
		
		tile = readTile(this.xCoord, this.yCoord, temp_width, temp_height, planeId);
		System.out.println(tile.length +"="+(temp_width * temp_height *3));
		if(nextRow && lastRow)
		{	
			this.xCoord = width;
		    this.yCoord = height;
		}
		else if(nextRow)
		{	
			this.xCoord = 0;
		    this.yCoord += tileHeight;
		}
		else 
		{
			this.xCoord += tileWidth;	
		}
		return tile;
	}

	public void close()throws FormatException, IOException
	{
		reader.close();
		
	}
	public static void main(String args[]) throws FormatException, IOException
	{
		
		SVSTileReader readerObject = new SVSTileReader("/Users/eyildirim/Documents/CNN_dspaces/camelyon-train-tumor/Tumor_001.tif");
		readerObject.setTileWidth(1024);
		readerObject.setTileHeight(1024);
		readerObject.setSeries(2);
		byte []tile = null;
		SVSKey key;
		SVSValue value;
		long tileId = 0;
		long xcoord = 0;
		long ycoord = 0;
		int tileWidth = 1024;
		int tileHeight = 1024;
		while((tile = readerObject.getNextTile())!=null)
		{
			//Create an SVS Key and Value object store them on a sequence file 
			key = new SVSKey();
			key.svsFileName = new Text( "/Users/eyildirim/Documents/input/TCGA-EJ-A46H-01Z-00-DX1.0E60D106-3766-49A8-B1D8-9BB216F06B2B.svs");
			key.tileID = new LongWritable(tileId++);
			key.xcoord = new LongWritable(xcoord);
			key.ycoord = new LongWritable(ycoord);
			if(xcoord + tileWidth >= readerObject.getWidth())
				tileWidth = (int) (readerObject.getWidth() - xcoord);
			key.tileWidth = new IntWritable(tileWidth);
			if(ycoord + tileHeight >= readerObject.getHeight())
				tileHeight = (int)(readerObject.getHeight()- ycoord);
			key.tileHeight = new IntWritable(tileHeight);	
			
			value = new SVSValue();
			value.RGBBytes = new BytesWritable(tile, tile.length);
			
		}
		readerObject.close();
	
	}
}
