package edu.rutgers.rdi2.dl4j;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import edu.rutgers.rdi2.bioformats_hdfs.mrjob.IntArrayWritable;
import edu.rutgers.rdi2.bioformats_hdfs.mrjob.LoadSequenceFile.ConvertToNativeTypes;
import edu.rutgers.rdi2.bioformats_hdfs.mrjob.LoadSequenceFile.NativeKeyType;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;
import scala.Tuple2;
import org.nd4j.linalg.lossfunctions.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
//import org.deeplearning4j.mlp.sequence.FromSequenceFilePairFunction;
//import org.deeplearning4j.mlp.sequence.ToSequenceFilePairFunction;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.api.Repartition;
import org.deeplearning4j.spark.api.RepartitionStrategy;
import org.deeplearning4j.spark.api.TrainingMaster;
import org.deeplearning4j.spark.api.stats.SparkTrainingStats;
import org.deeplearning4j.spark.data.DataSetExportFunction;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.deeplearning4j.spark.stats.StatsUtils;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;
 
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class FirstDNNExample {

	//  @Parameter(names="-preprocessData", description = "Whether data should be saved and preprocessed (set to false to use already saved data)", arity = 1)
	  // private boolean preprocessData = true;
	  
	//   @Parameter(names="-dataSavePath", description = "Directory in which to save the serialized data sets - required. For example, file:/C:/Temp/MnistMLPPreprocessed/", required = true)
	//      private String dataSavePath;
	  
	//      @Parameter(names="-useSparkLocal", description = "Use spark local (helper for testing/running without spark submit)", arity = 1)
	      private boolean useSparkLocal = true;
	  
	//      @Parameter(names="-batchSizePerWorker", description = "Number of examples to fit each worker with")
	      private int batchSizePerWorker = 1;
	  
	//      @Parameter(names="-numEpochs", description = "Number of epochs for training")
	      private int numEpochs = 10;
	      public static class NativeKeyType
	  	{
	  		public NativeKeyType(){}
	  		public NativeKeyType(int tw, int th, long id, int l, long x, long y, String s){
	  			tilewidth = tw;
	  			tileheight = th;
	  			tileID = id;
	  			xcoord = x;
	  			ycoord = y;
	  			filepath = s;
	  			label = l;
	  		}
	  		public int tilewidth;
	  		public int tileheight; 
	  		public long tileID;
	  		public long xcoord;
	  		public long ycoord;
	  		public String filepath;
	  		public int label;

	  	}
	      public static class ConvertToNativeTypes implements PairFunction<Tuple2<SVSKey, IntArrayWritable>, NativeKeyType, int[]> {
	  		public Tuple2<NativeKeyType, int[]> call(Tuple2<SVSKey, IntArrayWritable> record) {
	  			int [] intarray = new int[record._1.tileWidth.get() * record._1.tileHeight.get()];
	  			Writable []array = record._2.get();
	  			for(int i = 0; i < array.length; i++)
	  			{
	  				intarray[i] = ((IntWritable)array[i]).get();

	  			}
	  			SVSKey temp = record._1;
	  			NativeKeyType key = new NativeKeyType(temp.tileWidth.get(), temp.tileHeight.get(),temp.tileID.get(), temp.label.get(),temp.xcoord.get(),temp.ycoord.get(), temp.svsFileName.toString());
	  			return new Tuple2<NativeKeyType, int[]>(key, intarray);
	  		}
	  	}
	      public static class FromSequenceFilePairFunction implements Function<Tuple2<NativeKeyType,int []>,DataSet> {
	    	  //  @Override
	    	    public DataSet call(Tuple2<NativeKeyType, int  []> r) throws Exception {
	    	        int numExamples = 1;
	    	        boolean binarize = true;
	    	        float[][] featureData = new float[numExamples][0];
	    	        float[][] labelData = new float[numExamples][0];

	    	        int actualExamples = 0;
	    	        for( int i=0; i<numExamples; i++){
	    	           // if(!hasMore()) break;

	    	        	NativeKeyType key = r._1();
	    	        	int label = key.label;
	    	        	int []array = r._2();
	    	        	//convert int array to byte array
	    	        	ByteBuffer byteBuffer = ByteBuffer.allocate(array.length * 4);        
	    	            IntBuffer intBuffer = byteBuffer.asIntBuffer();
	    	            intBuffer.put(array);

	    	            byte[] img = byteBuffer.array();

	    	           // byte[] img = man.readImageUnsafe(order[cursor]);
	    	         

	    	            float[] featureVec = new float[img.length];
	    	            featureData[actualExamples] = featureVec;
	    	            labelData[actualExamples] = new float[2];//only two types of labels 0 and 1
	    	            labelData[actualExamples][label] = 1.0f;

	    	            for( int j=0; j<img.length; j++ ){
	    	                float v = ((int)img[j]) & 0xFF; //byte is loaded as signed -> convert to unsigned
	    	                if(binarize){
	    	                    if(v > 30.0f) featureVec[j] = 1.0f;
	    	                    else featureVec[j] = 0.0f;
	    	                } else {
	    	                    featureVec[j] = v/255.0f;
	    	                }
	    	            }

	    	            actualExamples++;
	    	        }

	    	        if(actualExamples < numExamples){
	    	            featureData = Arrays.copyOfRange(featureData,0,actualExamples);
	    	            labelData = Arrays.copyOfRange(labelData,0,actualExamples);
	    	        }

	    	        INDArray features = Nd4j.create(featureData);
	    	        INDArray labels = Nd4j.create(labelData);
	    	        DataSet ds = new DataSet(features,labels);
	    	        return ds;
	    	    }
	    	}
	      
	      public static void main(String args[]) throws Exception
	      {
	    	  new FirstDNNExample().entryPoint(args);
	    	  
	      }
	      protected void entryPoint(String[] args) throws Exception {
	    	    /*     JCommander jcmdr = new JCommander(this);
	    	          try{
	    	              jcmdr.parse(args);
	    	          } catch(ParameterException e){
	    	              //User provides invalid input -> print the usage info
	    	              jcmdr.usage();
	    	              try{ Thread.sleep(500); } catch(Exception e2){ }
	    	              throw e;
	    	          }
	    	  */
	    	       //   SparkConf sparkConf = new SparkConf();
	    	      //    if(useSparkLocal) sparkConf.setMaster("local[*]");
	    	       //   sparkConf.setAppName("MLP");
	    	       //   JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    	  
	    	          SparkConf conf = new SparkConf().setMaster("local").setAppName("Basic_App");
	    	  		conf.setSparkHome(System.getenv("SPARK_HOME"));
	    	  		
	    	  		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	    	  		conf.set("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator");
	    	  		conf.registerKryoClasses(new Class<?>[]{
	    	  			Class.forName("org.apache.hadoop.io.LongWritable"),
	    	  			Class.forName("org.apache.hadoop.io.IntWritable"),
	    	  			Class.forName("org.apache.hadoop.io.ArrayWritable"),
	    	  			Class.forName("edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey"),
	    	  			Class.forName("edu.rutgers.rdi2.bioformats_hdfs.mrjob.IntArrayWritable")
	    	  		});
	    	  		//conf.set("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator");
	    	  		
	    	  		JavaSparkContext sc = new JavaSparkContext(conf);


	    	  		JavaPairRDD<SVSKey, IntArrayWritable> input1 = sc.sequenceFile("hdfs://evenstar.rdi2.rutgers.edu:9000/camelyon-seq-subdataset/Tumor_001.tif.seq", SVSKey.class, IntArrayWritable.class);
	    	  		JavaPairRDD<NativeKeyType, int []> input2 = input1.mapToPair(new ConvertToNativeTypes());
	    	          
	    	          
	    	          
	    	          
	    	          //First: preprocess data into a sequence file
	    	      /*    if(preprocessData) {
	    	              DataSetIterator iter = new MnistDataSetIterator(batchSizePerWorker, true, 12345);
	    	              List<DataSet> list = new ArrayList<DataSet>();
	    	              while (iter.hasNext()) {
	    	                  list.add(iter.next());
	    	              }
	    	  
	    	              JavaRDD<DataSet> rdd = sc.parallelize(list);
	    	              JavaPairRDD<Text,BytesWritable> forSequenceFile = rdd.mapToPair(new ToSequenceFilePairFunction());
	    	              forSequenceFile.saveAsHadoopFile(dataSavePath, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);
	    	          }
	    	  
	    	          //Second: load the data from a sequence file
	    	          JavaPairRDD<Text,BytesWritable> sequenceFile = sc.sequenceFile(dataSavePath, Text.class, BytesWritable.class);
	    	        */
	    	  		JavaRDD<DataSet> trainData = input2.map(new FromSequenceFilePairFunction());
	    	  
	    	  
	    	          //----------------------------------
	    	          //Second: conduct network training
	    	  
	    	          //Network configuration:
	    	          MultiLayerConfiguration confNetwork = new NeuralNetConfiguration.Builder()
	    	              .seed(12345)
	    	              .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
	    	              .iterations(1)
	    	              .activation("relu")
	    	              .weightInit(WeightInit.XAVIER)
	    	              .learningRate(0.0069)
	    	              .updater(Updater.NESTEROVS).momentum(0.9)
	    	              .regularization(true).l2(1e-4)
	    	              .list()
	    	              .layer(0, new DenseLayer.Builder().nIn(512*512*4).nOut(50).build())
	    	              .layer(1,  new DenseLayer.Builder().nIn(50).nOut(20).build())
	    	              .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
	    	                  .activation("softmax").nIn(20).nOut(2).build())
	    	              .pretrain(false).backprop(true)
	    	              .build();
	    	        
	    	          //Configuration for Spark training: see http://deeplearning4j.org/spark for explanation of these configuration options
	    	          TrainingMaster tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)
	    	              .averagingFrequency(10)
	    	              .saveUpdater(true)
	    	              .workerPrefetchNumBatches(2)
	    	              .batchSizePerWorker(batchSizePerWorker)
	    	              .repartionData(Repartition.Always)
	    	              .repartitionStrategy(RepartitionStrategy.SparkDefault)
	    	              .build();
	    	  
	    	          SparkDl4jMultiLayer sparkNet = new SparkDl4jMultiLayer(sc, confNetwork, tm);
	    	          sparkNet.setCollectTrainingStats(true);
	    	  
	    	          //Execute training:
	    	          for( int i=0; i<numEpochs; i++ ){
	    	              sparkNet.fit(trainData);
	    	              System.out.println("Completed Epoch " + i);
	    	          }
	    	  
	    	          SparkTrainingStats stats = sparkNet.getSparkTrainingStats();
	    	          StatsUtils.exportStatsAsHtml(stats, "SparkStats.html", sc);
	    	  
	    	          System.out.println("----- DONE -----");
	    	      }
}

