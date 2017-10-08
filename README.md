# ParallelWSIConverter
A native YARN application that transfers WSI files from different URLS and converts them into binary sequence files

This library contains classes for a native YARN application that runs on the Hadoop ecosystem

It transfers whole slide image(WSI) datasets from different URLs (HTTP, S3, HDFS) in parallel and converts them to structured binary sequence files for distributed data access. The WSIs are very large images that can be of size 200000x200000 pixels.

Update the pom.xml with the proper dependencies in your own system and reconstruct the jar file

Prepare a list of WSI urls in the form of a txt file and upload it to HDFS in your own system: hdfs:///hdfspath-jbhi-svs.txt

Example URLS in .txt file : http://www.xxx.com/a.svs

                        s3n://bucket-name/dir-name/
                        
                        hdfs://localhost:9000/dir-name/
Upload jar file into HDFS : hdfs:///bioformatshdfs-0.0.1-SNAPSHOT-jar-with-dependencies.jar

Provide a local tmp directory for conversion process: /Users/eyildirim/Documents/tmp/

Provide a destination directory to store sequence files : hdfs:///jbhi-seq-l1/

Provide tile height and width and resolution level parameters of your choice: 1024 1024 1

Run the following command:

hadoop jar workspace/bioformats-hdfs/target/bioformats-hdfs-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.rutgers.rdi2.yarn_distributed_transfer.v2.Client hdfs:///hdfspath-jbhi-svs.txt /Users/eyildirim/Documents/tmp/ hdfs:///bioformatshdfs-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs:///jbhi-seq-l1/ 1024 1024 1
