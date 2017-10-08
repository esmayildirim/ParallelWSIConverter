package edu.rutgers.rdi2.bioformats_hdfs.urls;

import java.util.List;

import org.apache.hadoop.fs.Path;

public abstract class GenericURL {
	private String urlPath = "";
	private String serverPath = "";
	private String fileDirPath = "";
	private String fileName = "";
	public GenericURL(String path)
	{
		urlPath = path;
	}
	public void setFileName(String name)
	{
		fileName = name;
	}
	public String getFileName()
	{
		return fileName;
	}
	public void setServerPath(String path)
	{
		serverPath = path;
	}
	public String getServerPath()
	{
		return serverPath;
	}
	public void setURL(String urlParam)
	{
		urlPath = urlParam;	
	}
	public String getURL()
	{
		return urlPath;
	}
	public void setFileDirPath(String path)
	{
		fileDirPath = path;
	}
	public String getFileDirPath()
	{
		return fileDirPath;
	}
	public abstract List<Path> getRecursiveFileList(Path url) throws Exception;
    public abstract void parseServerPath() throws Exception;
    public abstract boolean isFile()throws Exception;
    public abstract boolean isDir() throws Exception;
    public void parseFileName() throws Exception
    {
    	setFileName(getURL().substring(getURL().lastIndexOf("/")+1));	
    }
   
}
