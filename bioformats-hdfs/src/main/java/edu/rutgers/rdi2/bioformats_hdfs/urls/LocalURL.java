package edu.rutgers.rdi2.bioformats_hdfs.urls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class LocalURL extends GenericURL{
  Configuration conf = new Configuration();  
	
    public LocalURL(String urlPath)
	{
		super(urlPath);		
	}
	public void parseServerPath() throws Exception
	{
		if(getURL().startsWith("/") || getURL().startsWith("/"))
		{
			setServerPath("/");
		}
		else
			throw new Exception();
	}
	public List<Path> getRecursiveFileList(Path url) throws IOException
	{
		List<Path> arraylist = new ArrayList<Path>();
		LocalFileSystem fs = FileSystem.getLocal(conf);
		recursivePreparePathList(arraylist, url, fs,conf);
		return arraylist;
		
	}
	public void recursivePreparePathList(List<Path> list, Path p,LocalFileSystem fs, Configuration conf) throws IOException
	{
		if(fs.isDirectory(p))
		{
			FileStatus[] fileStatus = fs.listStatus(p);
            Path[] paths = FileUtil.stat2Paths(fileStatus);
            for(Path path : paths)
                recursivePreparePathList(list,path,fs,conf);
            
		}
		else
		{   
			if(p.getName().endsWith(".seq"))
				list.add(p);
		}
		
	}
	public boolean isFile() throws IOException
	{
		FileSystem fs = new LocalFileSystem();
		boolean isFile =  !fs.isDirectory(new Path(getURL()));
		fs.close();
		return isFile;
	}
	public boolean isDir() throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		boolean isDir = fs.isDirectory(new Path(getURL()));
		fs.close();
		return isDir;
	}
	public static void main(String args[]) throws Exception
	{
		GenericURL url = new LocalURL("/Users/eyildirim/Documents/input");
		url.parseServerPath();
		System.out.println("ServerPath "+url.getServerPath());
		System.out.println(url.isDir());
		
		
	}
}
