package edu.rutgers.rdi2.bioformats_hdfs.urls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.fs.Path;


public class HTTPURL extends GenericURL{

	public HTTPURL(String urlPath)
	{
		super(urlPath);		
	}
	@Override
	public void parseServerPath() throws Exception
	{
		if(getURL().startsWith("http://")||getURL().startsWith("http://"))
		{
			
			Pattern pattern = Pattern.compile("http(.*)://(.*)/");
			Matcher matcher = pattern.matcher(getURL());
			if(matcher.matches())
				setServerPath (matcher.group(2));
			
		}
		else
			throw new Exception();
	}
	public List<Path> getRecursiveFileList(Path url) throws IOException
	{
		List<Path> arraylist = new ArrayList<Path>();
		arraylist.add(url);
		return arraylist;
		
	}
/*	public void recursivePreparePathList(List<Path> list, Path p,FileSystem fs, Configuration conf) throws IOException
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
	*/
	public boolean isFile() throws IOException //assume HTTP objects are always files
	{
		return true;
	}
	public boolean isDir() throws IOException
	{
		return false;
		
	}

}
