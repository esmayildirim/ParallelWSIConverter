package edu.rutgers.rdi2.yarn_distributed_transfer.v2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {  
	
	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  
  public void run(String args[]) throws Exception {
	  Configuration conf = new YarnConfiguration();
    /*Parameters obtained from client, some to be passed to the worker program */
	final String hdfsPathToTransferList = args[0];    
    final String outputFolder = args[1]; 
    final String jarPath = args[2];
 //   final int level = new Integer(args[3]).intValue();
 //   final long tileWidth = new Long(args[4]).longValue();
 //   final long tileHeight = new Long(args[5]).longValue();
    
    List<Path> URLs = TransferUtilities.getURLListing(hdfsPathToTransferList);//gets the list of files and folders to be transferred    
   // List<Path> URLs = new ArrayList<Path>();
    //URLs.add(new Path(hdfsPathToTransferList));
    List<Path> recursiveURLs = TransferUtilities.getRecursiveURLListing(URLs);//converts all folders to file URLs by recursive search
   // List<String> fileList = this.getFileList(hdfsPathToTransferList);
     
    String command = "";    
    
    // Initialize clients to ResourceManager and NodeManagers
    //Configuration conf = new YarnConfiguration();
    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    rmClient.registerApplicationMaster("", 0, "");    
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(128);
    capability.setVirtualCores(1);
    List<ContainerRequest> containerList = new ArrayList<ContainerRequest>();
    // Make container requests to ResourceManager
    for (int i = 0; i < recursiveURLs.size(); ++i) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      containerList.add(containerAsk);
      rmClient.addContainerRequest(containerAsk);
     
      
    }

    // Obtain allocated containers and launch 
    int allocatedContainers = 0;
    int completedContainers = 0;
    int responseID = 0;
    while (allocatedContainers < recursiveURLs.size()) {
      AllocateResponse response = rmClient.allocate(responseID++);
      for (Container container : response.getAllocatedContainers()) {
        //Command to execute to download url to HDFS
       // command = "java edu.rutgers.rdi2.bioformats_hdfs.urls.transfer.URLTransfer" +" " 
       //           + recursiveURLs.get(allocatedContainers).toString() + " " + outputFolder +" >> /Users/eyildirim/Documents/esma-urltransfer.log";
        command = "java edu.rutgers.rdi2.bioformats_hdfs.urls.transfer.URLTransfer" +" " 
                + recursiveURLs.get(allocatedContainers).toString() + " " + outputFolder +" >> /tmp/esma-urltransfer.log";

        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = 
            Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList(command));
        //Something crazy
     // Setup jar for Worker        
      LocalResource appJar = ApplicationUtilities.setupJar(new Path(jarPath), conf);
      ctx.setLocalResources(Collections.singletonMap(
                "transferservice.jar", appJar));

        // Setup CLASSPATH for Worker
        Map<String, String> appEnv = new HashMap<String, String>();
        ApplicationUtilities.setupAppEnv(appEnv, conf);
        ctx.setEnvironment(appEnv);
        
        //end
        nmClient.startContainer(container, ctx);
        ++allocatedContainers;
      }
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {        
          ++completedContainers;
          //LOG.error("Container Completed");
          System.out.println("Completed");
         
          //rmClient.removeContainerRequest(status.getContainerId());
        }
      Thread.sleep(100);
    }
    System.out.println("Allocated containers:"+allocatedContainers);
    // Now wait for containers to complete
  /*  int completedContainers = 0;
    System.out.println("Recursive URLs size:"+ allocatedContainers);
    while (completedContainers < allocatedContainers) {
    	//LOG.info("YELLOOOO");
    	//System.out.println("YELLOOOO");
      float progressIndicator = (float)(1.0*completedContainers/(1.0*allocatedContainers));
      System.out.println(progressIndicator);
      AllocateResponse response = rmClient.allocate(progressIndicator);
      
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {        
        ++completedContainers;
        //LOG.error("Container Completed");
        System.out.println("Completed");
       
        //rmClient.removeContainerRequest(status.getContainerId());
      }
      Thread.sleep(100);
    }
    */
    //LOG.info("Unregistering");
    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(
        FinalApplicationStatus.SUCCEEDED, "", "");
  }
  /*public List<String> getFileList( String srcURL) throws IOException
  {
	  List<String> list = new ArrayList<String>();
	  Configuration conf = new Configuration();
	  FileSystem fs = FileSystem.get(conf);
	  Path p = new Path(srcURL);
	  if(fs.isDirectory(p))
		{
		  FileStatus[] fileStatus = fs.listStatus(p);
          Path[] paths = FileUtil.stat2Paths(fileStatus);
          for(Path path : paths)
              list.add(path.toString());
             
		}
		else
		{   
			list.add(p.toString());
		}
	  fs.close();
	  return list;
  }
  */
  public static void main(String[] args) throws Exception
  {
	  ApplicationMaster master= new ApplicationMaster();
	  master.run(args);
	  
	  
  }
}

