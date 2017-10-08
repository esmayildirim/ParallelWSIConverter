package edu.rutgers.rdi2.bioformats_hdfs.urls.transfer;
           
import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import edu.rutgers.rdi2.bioformats_hdfs.urls.*;
import edu.rutgers.rdi2.yarn_distributed_transfer.v2.TransferUtilities;
/*
 * Uses proper protocol means to transfer files from different types of source urls
 * */
public class URLTransfer {
	private GenericURL srcURL;
	private GenericURL destURL;
	public URLTransfer(GenericURL src, GenericURL dest)
	{
		srcURL = src;
		destURL = dest;
	}
	public void transfer() throws Exception
	{
		System.out.println("Someones is calling me");
		if (srcURL instanceof HDFSURL && destURL instanceof LocalURL)
			 transferFromHDFStoLocal();
		else if(srcURL instanceof HTTPURL && destURL instanceof LocalURL)
			transferFromHTTPtoLocal();
		else if(srcURL instanceof HTTPURL && destURL instanceof HDFSURL)
			transferFromHTTPtoHDFS();
		else if(srcURL instanceof HDFSURL && destURL instanceof HDFSURL)
			transferFromHDFStoHDFS();                                 
		else if(srcURL instanceof LocalURL && destURL instanceof HDFSURL)
			transferFromLocaltoHDFS();		
		else if(srcURL instanceof S3URL && destURL instanceof LocalURL)
			transferFromS3toLocal();
	}
	public void transferFromHDFStoLocal() throws Exception
	{        
		System.out.println("transferfromHDFStolocal: server path"+srcURL.getServerPath());
	    
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(srcURL.getServerPath()), conf);//give a URI not local HDFS it could be somewhere else
       
        System.out.println("SRC:"+new Path(srcURL.getURL()).toString()+" DEST:"+new Path(destURL.getURL()).toString());
        fs.copyToLocalFile(new Path(srcURL.getURL()), new Path(destURL.getURL()));
        System.out.println("transfer finished");
        if(fs!=null)
        	fs.close();
	}
	public void transferFromHTTPtoLocal() throws Exception
	{
		//Configuration conf = new Configuration();
		//LocalFileSystem localFS = FileSystem.getLocal(conf);
		BufferedInputStream inStream = new BufferedInputStream(new URL(srcURL.getURL()).openStream());
		//FSDataOutputStream outStream = localFS.create(new Path(destURL.getURL() + "/" +srcURL.getURL().substring(srcURL.getURL().lastIndexOf("/")+1 )));//recursive structure can be kept as well
		DataOutputStream outStream = new DataOutputStream(new FileOutputStream(destURL.getURL() + "/" +srcURL.getURL().substring(srcURL.getURL().lastIndexOf("/")+1 )));
		System.out.println(destURL.getURL() + "/" +srcURL.getURL().substring(srcURL.getURL().lastIndexOf("/")+1 ));
		byte data[] = new byte[1024];
        int count;
		while ((count = inStream.read(data, 0, 1024)) != -1) {
	           outStream.write(data, 0, count);
	    }
		inStream.close();
        outStream.close();
        System.out.println("transferfromHTTPtolocal");
       // if(localFS!=null)
       // localFS.close();
	}
    public void transferFromHTTPtoHDFS() throws Exception
    { System.out.println("transferfromHTTPtoHDFS");
    	Configuration conf = new Configuration();
    	FileSystem destFS = FileSystem.get(new URI(destURL.getServerPath()), conf);
    	BufferedInputStream inStream = new BufferedInputStream(new URL(srcURL.getURL()).openStream());
        FSDataOutputStream outStream = destFS.create(new Path(destURL.getURL() + "/" + new Path(srcURL.getURL()).getName()));//recursive structure can be kept as well
        byte data[] = new byte[1024];
        int count;
		while ((count = inStream.read(data, 0, 1024)) != -1) {
	           outStream.write(data, 0, count);
	    }
		inStream.close();
        outStream.close();
        if(destFS!=null)
        destFS.close();
    }
    public void transferFromHDFStoHDFS() throws Exception
    { 
    	System.out.println("transferfromHDFStoHDFS");
    	Configuration conf = new Configuration();
    	System.out.println("SRC:"+new Path(srcURL.getURL()).toString()+" DEST:"+new Path(destURL.getURL()).toString());
        FileSystem srcFS = FileSystem.get(new URI(srcURL.getServerPath()), conf);//give a URI not local HDFS it could be somewhere else
        FileSystem destFS = FileSystem.get(new URI(destURL.getServerPath()), conf);
        System.out.println("server paths "+srcURL.getServerPath()+" "+destURL.getServerPath());
        
        FSDataInputStream inStream = new FSDataInputStream(srcFS.open(new Path(srcURL.getURL())));
        FSDataOutputStream outStream = destFS.create(new Path(destURL.getURL() + "/" + new Path(srcURL.getURL()).getName()));//recursive structure can be kept as well
        byte data[] = new byte[1024];
        int count;
        while ((count = inStream.read(data, 0, 1024)) != -1) {
           outStream.write(data, 0, count);
        }
        inStream.close();
        outStream.close();
        srcFS.close();
    	destFS.close();
    }
    public void transferFromLocaltoHDFS() throws Exception
    { 
    	System.out.println("transferfromLocaltoHDFS");
    	Configuration conf = new Configuration();
        //give a URI not local HDFS it could be somewhere else
       // LocalFileSystem localFS = FileSystem.getLocal(conf);
    	System.out.println("SRC:"+new Path(srcURL.getURL()).toString()+" DEST:"+new Path(destURL.getURL()).toString());
        FileSystem fs = FileSystem.get(new URI(destURL.getServerPath()), conf);
        fs.copyFromLocalFile(new Path(srcURL.getURL()), new Path(destURL.getURL() + "/" + new Path(srcURL.getURL()).getName()));
    
        if(fs!=null)
        fs.close();
    
    }
    public void transferFromS3toLocal() throws Exception
    {    
    	System.out.println("Transfer from S3 to Local");
    	Configuration conf = new Configuration();
    	System.out.println(srcURL.getServerPath());
    	System.out.println("SRC:"+srcURL.getURL());
    	System.out.println("DEST:"+destURL.getURL());
        FileSystem fs = FileSystem.get(new URI(srcURL.getServerPath()),conf);//give a URI not local HDFS it could be somewhere else
        
        System.out.println("SRC:"+new Path(srcURL.getURL()).toString()+" DEST:"+new Path(destURL.getURL()).toString());
       //try{
          fs.copyToLocalFile(new Path(srcURL.getURL()), new Path(destURL.getURL()));
    	 /*  System.out.println("OUTPUT URL"+destURL.getURL()+"/"+srcURL.getURL().substring(srcURL.getURL().lastIndexOf("/")+1));
    	   FSDataInputStream inStream = new FSDataInputStream(fs.open(new Path(srcURL.getURL())));
    	   System.out.println("file opened");
    	   DataOutputStream outStream = new DataOutputStream(new FileOutputStream(destURL.getURL()+"/"+srcURL.getURL().substring(srcURL.getURL().lastIndexOf("/")+1)));
    	   System.out.println("OUTPUT STREAM CONSTRUCTED");
    	   byte data[] = new byte[1024];
           int count;
           while ((count = inStream.read(data, 0, 1024)) != -1) {
              outStream.write(data, 0, count);
           }
           inStream.close();
           outStream.close();*/
       //}catch(Exception e)
       //{
    	 //  System.out.println("It threw an exception");
    	   
       //}
        if(fs!=null)
    	    fs.close();
    	
    	
    	
    }
    public static void main(String args[]) throws Exception
    {
    	
    	//String srct = new String("http://storage.googleapis.com/patents/grant_full_text/2014/ipg140107.zip");
    	GenericURL src = TransferUtilities.returnURLType(args[0]);
		src.setURL(args[0]);
		//String destt = new String("/Users/eyildirim/Documents/tmp");
    	GenericURL dest = TransferUtilities.returnURLType(args[1]);
    	dest.setURL(args[1]);
    	URLTransfer transferobject = new URLTransfer(src,dest);
    	transferobject.transfer();
    	
    	System.out.println("I am done");
    	//Next thing is uploading to HDFS as a sequence file. 
    	
    }
}
