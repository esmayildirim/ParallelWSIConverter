package edu.rutgers.rdi2.bioformats_hdfs;


import java.io.IOException;

import org.slf4j.*;

import ome.xml.meta.IMetadata;
import ome.xml.model.primitives.PositiveInteger;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.IFormatWriter;
import loci.formats.ImageReader;
import loci.formats.ImageWriter;
import loci.formats.MetadataTools;
import loci.formats.services.OMEXMLService;
import loci.formats.meta.MetadataStore;
public class SVSReaderWriter {
	  private static final Logger LOGGER = LoggerFactory.getLogger(SVSReaderWriter.class);

	public static void main(String[] args) throws FormatException, IOException
	{
		//Setup the reader
		// create a reader that will automatically handle any supported format
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
		  throw new FormatException("Could not create OME-XML store.", exc);
		  
		}
		catch (ServiceException exc) {
			reader.close();
		  throw new FormatException("Could not create OME-XML store.", exc);
		}

		reader.setMetadataStore(metadata);
		// initialize the dataset
		reader.setId("/Users/eyildirim/Documents/input/TCGA-EJ-A46H-01Z-00-DX1.0E60D106-3766-49A8-B1D8-9BB216F06B2B.svs");
		
		//Setup the writer
		// create a writer that will automatically handle any supported output format
		//IFormatWriter writer = new ImageWriter();
		// give the writer a MetadataRetrieve object, which encapsulates all of the
		// dimension information for the dataset (among many other things)
		//writer.setMetadataRetrieve(service.asRetrieve(reader.getMetadataStore()));		// initialize the writer
		//String filePrefix = "/Users/esmayildirim/Desktop/RUTGERS-RESEARCH/output/TCGA-output-";
		//writer.setId(filePrefix+".jpeg");
		
		//copy the image tile by tile
		int tileWidth = 1024;
		int tileHeight = 1024;
	
       int series;
		//for (int series=0; series<reader.getSeriesCount(); series++) 
		{ series = 2;
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

		       /* int tempTileWidth = 0;
		        int tempTileHeight = 0;
		        if((xCoordinate + tileWidth) > reader.getSizeX())
		        {
		        	lastTileWidth = true;
		        	tempTileWidth = reader.getSizeX() - xCoordinate;
		        }
		        if((yCoordinate + tileHeight) > reader.getSizeY())
		        {
		        	lastTileHeight = true;
		        	tempTileHeight = reader.getSizeY() - yCoordinate;
		        }
		        writer.changeOutputFile(filePrefix + series + "-" + image + "-" + row + "-" + col + ".tiff");
		        if(lastTileWidth == true && lastTileHeight == true)
		        {
		        	tile = reader.openBytes(image, xCoordinate, yCoordinate, tempTileWidth, tempTileHeight);
			        writer.saveBytes(image, tile,0,0,tempTileWidth, tempTileHeight);
			        System.out.println("True Coordinates: "+xCoordinate+" "+yCoordinate+" "+tempTileWidth+" "+tempTileHeight);

		        	
		        }
		        else if(lastTileWidth == true && lastTileHeight == false)
		        {
		        	tile = reader.openBytes(image, xCoordinate, yCoordinate, tempTileWidth, tileHeight);
			        writer.saveBytes(image, tile,0,0,tempTileWidth, tileHeight);
			        System.out.println("True Coordinates: "+xCoordinate+" "+yCoordinate+" "+tempTileWidth+" "+tileHeight);

		        }
		        else if(lastTileWidth == false && lastTileHeight == true)
		        {
		        	tile = reader.openBytes(image, xCoordinate, yCoordinate, tileWidth, tempTileHeight);
			        writer.saveBytes(image, tile,0,0,tileWidth, tempTileHeight);
			        System.out.println("True Coordinates: "+xCoordinate+" "+yCoordinate+" "+tileWidth+" "+tempTileHeight);

		        }*/	
		        //else{
		     /*   IFormatWriter writer = new ImageWriter();
				// give the writer a MetadataRetrieve object, which encapsulates all of the
				// dimension information for the dataset (among many other things)
				writer.setMetadataRetrieve(service.asRetrieve(reader.getMetadataStore()));		// initialize the writer
				String filePrefix = "/Users/esmayildirim/Desktop/RUTGERS-RESEARCH/output/TCGA-output-";
		        //writer.changeOutputFile(filePrefix + series + "-" + image + "-" + row + "-" + col + ".jpeg");
				writer.setId(filePrefix + series + "-" + image + "-" + row + "-" + col + ".jpg");
                writer.setSeries(series);
                */
		        //String[] outputFiles = new String[] {"/path/to/file/1.avi", "/path/to/file/2.avi"};
		        //int planesPerFile = reader.getImageCount() / outputFiles.length;
		        //for (int file=0; file<outputFiles.length; file++) {
		          ImageWriter writer = new ImageWriter();
		          String filePrefix = "/Users/eyildirim/Documents/output/TCGA-output-";
	                MetadataStore store = reader.getMetadataStore();
	                store.setPixelsSizeX(new PositiveInteger(tileWidth), 0);
	                store.setPixelsSizeY(new PositiveInteger(tileHeight), 0);
			          writer.setMetadataRetrieve(service.asRetrieve(store));
			          writer.setId(filePrefix + series + "-" + image + "-" + row + "-" + col + ".jpeg");

		         // for (int image=0; image<planesPerFile; image++) {
		         //   int index = file * planesPerFile + image;
			          int pixelType = reader.getPixelType();
			          int bitsPerPixel = reader.getBitsPerPixel();
			          tile = reader.openBytes(image, xCoordinate, yCoordinate, tileWidth, tileHeight);
			          LOGGER.info("Hello {} {} {}",bitsPerPixel, pixelType, tile.length);
			          if(pixelType == FormatTools.INT32)
			        	  LOGGER.info("It is INT32");
			          else if(pixelType == FormatTools.FLOAT)
			        	  LOGGER.info("It is FLOAT");
			          else if(pixelType == FormatTools.UINT8)
			        	  LOGGER.info("It is UINT8");
			          if(reader.isRGB())
			          {
			        	  LOGGER.info("It is RGB");
			        	  
			          }
			     //   
		         //   writer.saveBytes(image, tile);
		            
		            // }
		          writer.close();
		        //}
		        
		        
		        
		        
		       // }
		        
		        	
		      }
		    }
		  }
		}
		
		
		
		reader.close();
		//writer.close();
	}

}
