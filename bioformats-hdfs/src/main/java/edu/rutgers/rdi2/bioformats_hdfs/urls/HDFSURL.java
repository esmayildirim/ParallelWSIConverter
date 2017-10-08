package edu.rutgers.rdi2.bioformats_hdfs.urls;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HDFSURL extends GenericURL{
    public static final String DEFAULT_HADOOP_INSTALL = "/Users/eyildirim/Documents/hadoop-2.7.1/";
	//public static final String DEFAULT_HADOOP_INSTALL = "/home/hduser/hadoop-2.7.1/";
	private Configuration conf = new Configuration();
    public HDFSURL(String urlPath)
	{
		super(urlPath);
		try{
		parseServerPath();
		}catch(Exception e)
		{
			
		}
	}
	public void parseServerPath() throws Exception
	{
		if(getURL().startsWith("hdfs:///") || getURL().startsWith("/") ||getURL().startsWith("hdfs://"))
		{
			Map<String, String> env = System.getenv();
			String hadoopHome = env.get("HADOOP_INSTALL");
			
			System.out.println("Hadoop Home:" + hadoopHome);
			if (hadoopHome == null)
				hadoopHome = DEFAULT_HADOOP_INSTALL;
			conf.addResource(new Path(hadoopHome + "/etc/hadoop/conf/core-site.xml"));	//PATH object is necessary to look at the local file system
			setServerPath(conf.get("fs.defaultFS"));//This is not a good idea
		}
		else if(getURL().startsWith("hdfs://"))
		{
			Pattern pattern = Pattern.compile("hdfs://(.*)/");
			Matcher matcher = pattern.matcher(getURL());
			if(matcher.lookingAt())
			{
				setServerPath ("hdfs://"+matcher.group(1));
			    System.out.println("matches");
			}
			
		}
		else
			throw new Exception();
	}
	public List<Path> getRecursiveFileList(Path url) throws Exception
	{
		List<Path> arraylist = new ArrayList<Path>();
		System.out.println("before getting file system");
		FileSystem fs = FileSystem.get(new URI(new HDFSURL(url.toString()).getServerPath()), conf);
		System.out.println("after getting file system");
		recursivePreparePathList(arraylist, url, fs,conf);
		fs.close();
		return arraylist;
		
	}
	public void recursivePreparePathList(List<Path> list, Path p,FileSystem fs, Configuration conf) throws IOException
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
			list.add(p);
		}
		
	}
	public boolean isFile() throws Exception
	{
		FileSystem fs = FileSystem.get(new URI(this.getServerPath()),conf);
		boolean isFile = !fs.isDirectory(new Path(getURL()));
		fs.close();
		return isFile;
	}
	public boolean isDir() throws Exception
	{
		FileSystem fs = FileSystem.get(new URI(this.getServerPath()),conf);
		boolean isDir = fs.isDirectory(new Path(getURL()));
		fs.close();
		return isDir;
	}
	public static void main(String args[]) throws Exception
	{
		GenericURL url = new HDFSURL("hdfs:///patents-downloaded/");
		url.parseServerPath();
		System.out.println("ServerPath "+url.getServerPath());
		System.out.println(url.isDir());
		
		
	}
	
}
