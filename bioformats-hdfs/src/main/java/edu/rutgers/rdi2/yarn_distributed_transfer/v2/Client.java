package edu.rutgers.rdi2.yarn_distributed_transfer.v2;


import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
/*
 * Takes parameters from the users and launches ApplicationMasterAsync
 * */
public class Client {

    Configuration conf = new YarnConfiguration();

    public void run(String[] args) throws Exception {

    	final String hdfsPathToTransferList = args[0];
    	final String outputLocation = args[1];
        final Path hdfsJarPath = new Path(args[2]);
        final String seqFolder = args[3]; 
        final int tileWidth = new Integer(args[4]).intValue();
        final int tileHeight = new Integer(args[5]).intValue();
        final int series = new Integer(args[6]).intValue();
                        
        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(Collections.singletonList(" java "
                + " -Xmx128M" + " edu.rutgers.rdi2.yarn_distributed_transfer.v2.ApplicationMasterAsync"
                + " " + hdfsPathToTransferList + " " + outputLocation + " "+hdfsJarPath+" "+seqFolder
                + " " + tileWidth+" " +tileHeight+" "+ series+ " > /Users/eyildirim/Documents/esma-appmaster.log"));

        // Setup jar for ApplicationMaster        
        LocalResource appMasterJar = ApplicationUtilities.setupJar(hdfsJarPath, conf);
        amContainer.setLocalResources(Collections.singletonMap(
                "appmaster.jar", appMasterJar));

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        ApplicationUtilities.setupAppEnv(appMasterEnv, conf);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app
                .getApplicationSubmissionContext();
        appContext.setApplicationName("download-yarn-app"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED
                && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        System.out.println("Application " + appId + " finished with"
                + " state " + appState + "." + " The application started at "
                + appReport.getStartTime() + "."
                + " The application ended at  " + appReport.getFinishTime());

    }
/*
    private LocalResource setupAppMasterJar(Path jarPath)
            throws IOException {
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        return appMasterJar;
    }

    

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                    c.trim());
        }
        /*
         * Assuming our classes or jars are available as local resources in the
         * working directory from which the command will be run, we need to append
         * "." to the path.By default, all the hadoop specific classpaths will
         * already be available in $CLASSPATH, so we should be careful not to
         * overwrite it.
         */
 /*       Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*");
    }
*/
    public static void main(String[] args) throws Exception {
        Client c = new Client();
        c.run(args);
    }
}
