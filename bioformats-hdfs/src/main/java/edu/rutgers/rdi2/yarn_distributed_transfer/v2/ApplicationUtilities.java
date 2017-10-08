package edu.rutgers.rdi2.yarn_distributed_transfer.v2;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
/*
 * Used for setting up environment vars 
 */
public class ApplicationUtilities {
	
	public static LocalResource setupJar(Path jarPath, Configuration conf)
            throws IOException {
        LocalResource appJar = Records.newRecord(LocalResource.class);
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appJar.setType(LocalResourceType.FILE);
        appJar.setVisibility(LocalResourceVisibility.APPLICATION);
        appJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appJar.setSize(jarStat.getLen());
        appJar.setTimestamp(jarStat.getModificationTime());
        return appJar;
    }

    

    public static void setupAppEnv(Map<String, String> appEnv, Configuration conf) {
      
    	for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appEnv, Environment.CLASSPATH.name(),
                    c.trim());
        }
        /*
         * Assuming our classes or jars are available as local resources in the
         * working directory from which the command will be run, we need to append
         * "." to the path.By default, all the hadoop specific classpaths will
         * already be available in $CLASSPATH, so we should be careful not to
         * overwrite it.
         */
        Apps.addToEnvironment(appEnv, Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*");
    }


}
