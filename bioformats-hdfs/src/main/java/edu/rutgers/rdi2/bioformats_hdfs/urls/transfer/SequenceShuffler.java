package edu.rutgers.rdi2.bioformats_hdfs.urls.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

import edu.rutgers.rdi2.bioformats_hdfs.mrjob.IntArrayWritable;

import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;
import edu.rutgers.rdi2.yarn_distributed_transfer.v2.TransferUtilities;

public class SequenceShuffler {
public static void main(String[] args) throws Exception{
		
	    List<Path> normalURLs =new  ArrayList<Path>();
	    normalURLs.add(new Path(args[0]));
	 
        List<Path> recursiveNormalURLs = TransferUtilities.getRecursiveURLListing(normalURLs);
		
        List<Path> tumorURLs = new ArrayList<Path>();
        tumorURLs.add(new Path(args[1]));
        List<Path> recursiveTumorURLs = TransferUtilities.getRecursiveURLListing(tumorURLs);
        int poolSizeNormal = recursiveNormalURLs.size();
        int poolSizeTumor = recursiveTumorURLs.size();
		String dest = args[2];
    	
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Reader readerN = null;
		SequenceFile.Reader readerT = null;
		SequenceFile.Writer writer = null;
		int iN, iT;
		iN = 0; iT = 0;
		boolean nfinished , tfinished ; 
		nfinished = tfinished = false;
		int rb;
		try {		
			writer = SequenceFile.createWriter(fs, conf, new Path(dest), new SVSKey().getClass(), new  IntArrayWritable().getClass(), 
	    			SequenceFile.CompressionType.RECORD, new DefaultCodec());
			readerN = new SequenceFile.Reader(conf, Reader.file(recursiveNormalURLs.get(iN)), Reader.bufferSize(4096 * 4096), Reader.start(0));
			readerT = new SequenceFile.Reader(conf, Reader.file(recursiveTumorURLs.get(iT)), Reader.bufferSize(4096 * 4096), Reader.start(0));
			Random r = new Random();
			   while( iN < poolSizeNormal && iT < poolSizeTumor)
			   {
				   if(nfinished){
				       nfinished = false;
					   readerN = new SequenceFile.Reader(conf, Reader.file(recursiveNormalURLs.get(iN)), Reader.bufferSize(4096 * 4096), Reader.start(0));
				   }
				   if(tfinished){
					   tfinished = false;
					   readerT = new SequenceFile.Reader(conf, Reader.file(recursiveTumorURLs.get(iT)), Reader.bufferSize(4096 * 4096), Reader.start(0));
				   }
				   
				   Writable keyN = (Writable) ReflectionUtils.newInstance(new SVSKey().getClass(), conf);
				   Writable valueN = (Writable) ReflectionUtils.newInstance(new  IntArrayWritable().getClass(), conf);
				   Writable keyT = (Writable) ReflectionUtils.newInstance(new SVSKey().getClass(), conf);
				   Writable valueT = (Writable) ReflectionUtils.newInstance(new  IntArrayWritable().getClass(), conf);
				   rb = r.nextInt() % 2;
				   if(rb == 0)
				   {
					   if(readerN.next(keyN, valueN)){
						   writer.append(keyN, valueN);
					   }
					   else{
						   nfinished = true;
						   IOUtils.closeStream(readerN);
						   iN++;
					   }
				   }	   
				   else
				   {
					   if(readerT.next(keyT, valueT)){
						   writer.append(keyT, valueT);
					   }
					   else{
						   tfinished = true;
						   IOUtils.closeStream(readerT);
						   iT++;
					   }
				   }
				   
			   }
			   while(iN < poolSizeNormal)
			   {
				   Writable keyN = (Writable) ReflectionUtils.newInstance(new SVSKey().getClass(), conf);
				   Writable valueN = (Writable) ReflectionUtils.newInstance(new  IntArrayWritable().getClass(), conf);
				   if(nfinished){
					   nfinished = false;
					   readerN = new SequenceFile.Reader(conf, Reader.file(recursiveNormalURLs.get(iN)), Reader.bufferSize(4096 * 4096), Reader.start(0));	   
				   }
				   while(readerN.next(keyN,valueN)){
					   writer.append(keyN, valueN);
				   }
				   nfinished = true;
				   IOUtils.closeStream(readerN);
				   iN++;
			   }
			   while(iT < poolSizeTumor)
			   {
				   Writable keyT = (Writable) ReflectionUtils.newInstance(new SVSKey().getClass(), conf);
				   Writable valueT = (Writable) ReflectionUtils.newInstance(new  IntArrayWritable().getClass(), conf);
				   if(tfinished){
					   tfinished = false;
					   readerT = new SequenceFile.Reader(conf, Reader.file(recursiveTumorURLs.get(iT)), Reader.bufferSize(4096 * 4096), Reader.start(0));	   
				   }
				   while(readerT.next(keyT,valueT)){
					   writer.append(keyT, valueT);
				   }
				   tfinished = true;
				   IOUtils.closeStream(readerT);
				   iT++;
			   }
			   
				
				
				
		    	
		    	
				
		} finally {
			IOUtils.closeStream(writer);
		}		
	}
		
		

	}


