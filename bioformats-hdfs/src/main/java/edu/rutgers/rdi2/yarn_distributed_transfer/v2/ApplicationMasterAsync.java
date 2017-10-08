package edu.rutgers.rdi2.yarn_distributed_transfer.v2;


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

/* The class is responsible for starting YARN containers for YARN jobs
 * Each job runs an application of your choice in our case either SVSToSequenceFileUploader or 
 * CamelyonToSequenceFileUploader with their parameter settings
 * */
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler{  
	
  
	Configuration conf;
	NMClient nmClient;
	String command;
	int numContainersToWaitFor;
	List<Path> recursiveURLs;
	String outputFolder;
	String seqFolder;
	String jarPath;
	String hdfsPathToTransferList;
	int allocatedContainers;
	int tileWidth;
	int tileHeight;
	int series;
	public ApplicationMasterAsync(String command, int numContainersToWaitFor)
	{
		this.command = command;
		allocatedContainers = 0;
		conf = new YarnConfiguration();
		this.numContainersToWaitFor = numContainersToWaitFor;
		nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();
	}
	
	public void onContainersAllocated(List<Container> containers) {
        
		for (Container container : containers) {
            try {
            /*	this.command = "java edu.rutgers.rdi2.bioformats_hdfs.urls.transfer.SVSToSequenceFileUploader" +" " 
                        + this.recursiveURLs.get(allocatedContainers).toString() + " " + this.outputFolder + " " 
            			+ this.seqFolder+" "+ this.tileWidth +" "+this.tileHeight+" "+this.series
            			+ " >> /Users/eyildirim/Documents/esma-urltransfer.log";*/
            	this.command = "java edu.rutgers.rdi2.bioformats_hdfs.urls.transfer.CamelyonToSequenceFileUploader" +" " 
                        + this.recursiveURLs.get(allocatedContainers).toString() + " " + this.outputFolder + " " 
            			+ this.seqFolder+" "+ this.tileWidth +" "+this.tileHeight+" "+this.series
            			+ " >> /Users/eyildirim/Documents/esma-urltransfer.log";
            	synchronized (this) {
	                allocatedContainers++;
	            }
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(this.command));
                LocalResource appJar = ApplicationUtilities.setupJar(new Path(this.jarPath), conf);
                ctx.setLocalResources(Collections.singletonMap(
                          "transferservice.jar", appJar));

                  // Setup CLASSPATH for Worker
                  Map<String, String> appEnv = new HashMap<String, String>();
                  ApplicationUtilities.setupAppEnv(appEnv, this.conf);
                  ctx.setEnvironment(appEnv);
                
                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }
	  public void onContainersCompleted(List<ContainerStatus> statuses) {
	        for (ContainerStatus status : statuses) {
	            System.out.println("[AM] Completed container " + status.getContainerId());
	            synchronized (this) {
	                numContainersToWaitFor--;
	            }
	        }
	    }

	    public void onReboot() {
	    }

	    public void onShutdownRequest() {
	    }

	    public void onError(Throwable t) {
	    }

	    public float getProgress() {
	        return 0;
	    }

	    public boolean doneWithContainers() {
	        return numContainersToWaitFor == 0;
	    }

	    public Configuration getConfiguration() {
	        return conf;
	    }
	    public void onNodesUpdated(List<NodeReport> arg0) {
	    	// TODO Auto-generated method stub
	    	
	    }
	    public void runMainLoop(String args[]) throws Exception {
	    	long start = System.currentTimeMillis();
	    	this.hdfsPathToTransferList = args[0];    
	        this.outputFolder = args[1]; 
	        this.jarPath = args[2];
	        this.seqFolder = args[3];
	        this.tileWidth = new Integer(args[4]).intValue();
	        this.tileHeight = new Integer(args[5]).intValue();
	        this.series = new Integer(args[6]).intValue();
	        List<Path> URLs = TransferUtilities.getURLListing(this.hdfsPathToTransferList);
	        recursiveURLs = TransferUtilities.getRecursiveURLListing(URLs);
	        
	        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
	        rmClient.init(getConfiguration());
	        rmClient.start();

	        // Register with ResourceManager
	        System.out.println("[AM] registerApplicationMaster 0");
	        rmClient.registerApplicationMaster("", 0, "");
	        System.out.println("[AM] registerApplicationMaster 1");

	        // Priority for worker containers - priorities are intra-application
	        Priority priority = Records.newRecord(Priority.class);
	        priority.setPriority(0);

	        // Resource requirements for worker containers
	        Resource capability = Records.newRecord(Resource.class);
	        capability.setMemory(128);
	        capability.setVirtualCores(1);

	        this.numContainersToWaitFor = this.recursiveURLs.size();
	        // Make container requests to ResourceManager
	        for (int i = 0; i < numContainersToWaitFor; ++i) {
	            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
	            System.out.println("[AM] Making res-req " + i);
	            rmClient.addContainerRequest(containerAsk);
	        }

	        System.out.println("[AM] waiting for containers to finish");
	        while (!doneWithContainers()) {
	            Thread.sleep(100);
	        }

	        System.out.println("[AM] unregisterApplicationMaster 0");
	        // Un-register with ResourceManager
	        rmClient.unregisterApplicationMaster(
	                FinalApplicationStatus.SUCCEEDED, "", "");
	        System.out.println("[AM] unregisterApplicationMaster 1");
	        long end = System.currentTimeMillis();
	        System.out.println(end-start +"\t msecs");
	    }
	    
	    public static void main(String[] args) throws Exception {
	        
	        ApplicationMasterAsync master = new ApplicationMasterAsync("", 0);
	        master.runMainLoop(args);

	    }

  

}

