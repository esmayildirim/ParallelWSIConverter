package edu.rutgers.rdi2.bioformats_hdfs.urls;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class S3URL extends GenericURL{
	private Configuration conf = new Configuration(); 
	public S3URL(String urlPath)
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
			if(getURL().startsWith("s3://") || getURL().startsWith("s3n://") ||getURL().startsWith("s3a://"))
			{
				
				Pattern pattern = Pattern.compile("s3(.*)://(.*)/");
				Matcher matcher = pattern.matcher(getURL());
				if(matcher.lookingAt())
				{
					setServerPath ("s3"+matcher.group(1)+"://"+matcher.group(2)+"/");
				    System.out.println("matches: s3"+matcher.group(1)+"://"+matcher.group(2)+"/");
				}
				
			}
			else
				throw new Exception();
	}
	
	 public List<Path> getRecursiveFileList(Path url) throws IOException, URISyntaxException
		{
			List<Path> arraylist = new ArrayList<Path>();
			System.out.println("before getting file system");
			FileSystem fs = FileSystem.get(new URI(new S3URL(url.toString()).getServerPath()), conf);
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
			GenericURL url = new S3URL("s3n://svsimage-esma/TCGA-HC-A4ZV-01Z-00-DX1.74103CE2-AD31-4DAE-A812-47568B9BD19E.svs");
			
			System.out.println("ServerPath "+url.getServerPath());
			System.out.println(url.isDir());
			
			
		}
	 
}
