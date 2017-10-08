package edu.rutgers.rdi2.dl4j;



import edu.rutgers.rdi2.bioformats_hdfs.mrjob.IntArrayWritable;
import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;
import scala.Tuple2;
import org.nd4j.linalg.lossfunctions.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.conf.layers.ConvolutionLayer;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.conf.layers.SubsamplingLayer;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.api.Repartition;
import org.deeplearning4j.spark.api.RepartitionStrategy;
import org.deeplearning4j.spark.api.TrainingMaster;
import org.deeplearning4j.spark.api.stats.SparkTrainingStats;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.deeplearning4j.spark.stats.StatsUtils;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;


public class CNNExample {


	//      @Parameter(names="-useSparkLocal", description = "Use spark local (helper for testing/running without spark submit)", arity = 1)
	private boolean useSparkLocal = true;

	//      @Parameter(names="-batchSizePerWorker", description = "Number of examples to fit each worker with")
	private int batchSizePerWorker = 1;

	//      @Parameter(names="-numEpochs", description = "Number of epochs for training")
	private int numEpochs = 2;
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
	
	public static class CNNTensorPairFunction implements Function<Tuple2<NativeKeyType,int []>,DataSet> {
		//  @Override
		public DataSet call(Tuple2<NativeKeyType, int  []> r) throws Exception {
			int numExamples = 1;
			int nChannels, height, width;
			boolean binarize = true;
			double []featureData = new double[numExamples * 512 * 512 * 4];
			float[][] labelData = new float[numExamples][0];

			int actualExamples = 0;
			for( int i=0; i<numExamples; i++){
				// if(!hasMore()) break;
              
				NativeKeyType key = r._1();
				 nChannels = 4;
				 height = key.tileheight;
				 width = key.tilewidth;
				int label = key.label;
				int []array = r._2();
				//convert int array to byte array
				ByteBuffer byteBuffer = ByteBuffer.allocate(array.length * 4);        
				IntBuffer intBuffer = byteBuffer.asIntBuffer();
				intBuffer.put(array);

				byte[] img = byteBuffer.array();
			//	featureData[i] = new float[nChannels * height * width];
				int k = 0;
				for(int h = 0; h < height; h++)
					for(int w = 0; w < width; w++)
					{
						for(int c = 0; c < nChannels; c ++)
						{
							double v = ((int)img[k++]) & 0xFF;
							if(binarize){
								if(v > 30.0f) featureData[i * nChannels * height * width + c * height * width + h * width + w] = 1.0f;
								else featureData[i * nChannels * height * width + c * height * width + h * width + w] = 0.0f;
							} else {
								featureData[i * nChannels * height * width + c * height * width + h * width + w] = v/255.0f;
							}
						}
					}
			
				labelData[actualExamples] = new float[2];//only two types of labels 0 and 1
				labelData[actualExamples][label] = 1.0f;
                
				

				actualExamples++;
			}

			if(actualExamples < numExamples){
				featureData = Arrays.copyOfRange(featureData,0,actualExamples);
				labelData = Arrays.copyOfRange(labelData,0,actualExamples);
			}

			INDArray features = Nd4j.create(featureData, new int[]{actualExamples, 4, 512,512}, 'c');
			INDArray labels = Nd4j.create(labelData);
			DataSet ds = new DataSet(features,labels);
			return ds;
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
		new CNNExample().entryPoint(args);

	}
	protected void entryPoint(String[] args) throws Exception {

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


		JavaSparkContext sc = new JavaSparkContext(conf);


		JavaPairRDD<SVSKey, IntArrayWritable> input1 = sc.sequenceFile("hdfs://evenstar.rdi2.rutgers.edu:9000/camelyon-train-small-seq/", SVSKey.class, IntArrayWritable.class);
		JavaPairRDD<NativeKeyType, int []> input2 = input1.mapToPair(new ConvertToNativeTypes());
		JavaRDD<DataSet> trainData = input2.map(new FromSequenceFilePairFunction());
        
		JavaPairRDD<SVSKey, IntArrayWritable> input3 = sc.sequenceFile("hdfs://evenstar.rdi2.rutgers.edu:9000/camelyon-test-small-seq/", SVSKey.class, IntArrayWritable.class);
		JavaPairRDD<NativeKeyType, int []> input4 = input3.mapToPair(new ConvertToNativeTypes());
		JavaRDD<DataSet> testData = input4.map(new FromSequenceFilePairFunction());

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
         /*
		 MultiLayerConfiguration confNetwork = new NeuralNetConfiguration.Builder()
	                .seed(124)
	                .iterations(1) // Training iterations as above
	                .regularization(true).l2(0.0005)
	                /*
	                    Uncomment the following for learning decay and bias
	                 */
	    /*            .learningRate(.01)//.biasLearningRate(0.02)
	                //.learningRateDecayPolicy(LearningRatePolicy.Inverse).lrPolicyDecayRate(0.001).lrPolicyPower(0.75)
	                .weightInit(WeightInit.XAVIER)
	                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
	                .updater(Updater.NESTEROVS).momentum(0.9)
	                .list()
	                .layer(0, new ConvolutionLayer.Builder(5, 5)
	                        //nIn and nOut specify depth. nIn here is the nChannels and nOut is the number of filters to be applied
	                        .nIn(4) //number of channels
	                        .stride(1, 1)
	                        .nOut(20)
	                        .activation("identity")
	                        .build())
	                .layer(1, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX)
	                        .kernelSize(2,2)
	                        .stride(2,2)
	                        .build())
	                .layer(2, new ConvolutionLayer.Builder(5, 5)
	                        //Note that nIn need not be specified in later layers
	                        .stride(1, 1)
	                        .nOut(50)
	                        .activation("identity")
	                        .build())
	                .layer(3, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX)
	                        .kernelSize(2,2)
	                        .stride(2,2)
	                        .build())
	                .layer(4, new DenseLayer.Builder().activation("relu")
	                        .nOut(500).build())
	                .layer(5, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
	                        .nOut(2) //number of classes
	                        .activation("softmax")
	                        .build())
	                .setInputType(InputType.convolutional(512,512,4)) //See note below
	                .backprop(true).pretrain(false)
	                .build();
		*/
	
		
		//Configuration for Spark training: see http://deeplearning4j.org/spark for explanation of these configuration options
		TrainingMaster tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)
				.averagingFrequency(10)
				.saveUpdater(true)
				.workerPrefetchNumBatches(10)
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

		 Evaluation evaluation = sparkNet.evaluate(testData);
	     System.out.println("***** Evaluation *****");
	     System.out.println(evaluation.stats());
	       
		System.out.println("----- DONE -----");
	}
}

