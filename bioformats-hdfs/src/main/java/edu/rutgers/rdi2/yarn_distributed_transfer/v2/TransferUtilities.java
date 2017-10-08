package edu.rutgers.rdi2.yarn_distributed_transfer.v2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import edu.rutgers.rdi2.bioformats_hdfs.urls.*;
/* Creates proper URL objects based on the syntax of the urls for files */

public class TransferUtilities {
    
    public static boolean doesFileExists(FileSystem fs,Path outFile) throws Exception {
        
        return fs.exists(outFile);
    }
    
    public static String getFilePathFromURL(String rootPath, String fileUrl){
        String filePath =  rootPath + "/" + fileUrl.substring(fileUrl.lastIndexOf("/")+1);
        return filePath;
    }
    public static List<Path> getURLListing(String hdfsTransferList) throws Exception{
        List<Path> lines = new ArrayList<Path>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(hdfsTransferList);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(p)));
        String line;
        line=br.readLine();
        
        if(!StringUtils.isBlank(line)){
            lines.add(new Path(line));
            System.out.println(line);
        }
        while (line != null){                
                line=br.readLine();
                if(!StringUtils.isBlank(line)){
                    lines.add(new Path(line));
                    System.out.println(line);
                }
        }
        br.close();
        fs.close();
        return lines;
        
    }
    public static List<Path> getRecursiveURLListing(List<Path> URLs) throws Exception
    {
    	List<Path> extendedURLs = new ArrayList<Path>();
    	int i;
    	for(i = 0; i< URLs.size(); i++){
    		String path = ((Path)URLs.get(i)).toString();
    		System.out.println("PATH given:"+path);
    		GenericURL newURL = TransferUtilities.returnURLType(path);
    		extendedURLs.addAll(newURL.getRecursiveFileList(new Path(path)));
    	}
        return extendedURLs;
    }
    public static GenericURL returnURLType(String url)
	{
		if(url.startsWith("hdfs://")||url.startsWith("hdfs:/"))
			return new HDFSURL(url);
		else if(url.startsWith("http://")||url.startsWith("https://"))
			return new HTTPURL(url);
		else if(url.startsWith("s3n://")||url.startsWith("s3a://")||url.startsWith("s3://"))
			return new S3URL(url);
		return new LocalURL(url);
	}
}
