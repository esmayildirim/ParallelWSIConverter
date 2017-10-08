package edu.rutgers.rdi2.bioformats_hdfs.mrjob;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;
import scala.Tuple2;

public class LoadSequenceFile {
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

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Basic_App");
		conf.setSparkHome(System.getenv("SPARK_HOME"));

		conf.registerKryoClasses(new Class<?>[]{
			Class.forName("org.apache.hadoop.io.LongWritable"),
			Class.forName("org.apache.hadoop.io.IntWritable"),
			Class.forName("org.apache.hadoop.io.ArrayWritable"),
			Class.forName("edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey"),
			Class.forName("edu.rutgers.rdi2.bioformats_hdfs.mrjob.IntArrayWritable")
		});
	
		JavaSparkContext sc = new JavaSparkContext(conf);


		JavaPairRDD<SVSKey, IntArrayWritable> input1 = sc.sequenceFile("hdfs://evenstar.rdi2.rutgers.edu:9000/camelyon-seq-subdataset/Tumor_001.tif.seq", SVSKey.class, IntArrayWritable.class);
		JavaPairRDD<NativeKeyType, int []> input2 = input1.mapToPair(new ConvertToNativeTypes());
		JavaRDD<NativeKeyType> result = input2.map(new Function<Tuple2<NativeKeyType,int[]>, NativeKeyType>() {
			public NativeKeyType call(Tuple2<NativeKeyType,int[]> x) { 
			
			  NativeKeyType key = x._1;
			  int [] pixels = x._2;
				BufferedImage img=new BufferedImage(key.tilewidth, key.tileheight, BufferedImage.TYPE_INT_ARGB);
						//System.out.println(key.tileID+" "+key.filepath+" " +key.label+" "+key.xcoord+" "+key.ycoord+ " " +key.tilewidth+ " "+key.tileheight);
			  img.setRGB(0, 0, key.tilewidth, key.tileheight, pixels, 0, key.tilewidth);
			 try{ ImageIO.write(img, "png", new File("/Users/eyildirim/Documents/output2/"+Math.random()+key.tileID+".png"));
			 
			 }catch(IOException e){}
			  return key;
			
			
			} });
		for(NativeKeyType key: result.collect())
		{
			System.out.println(key.tileID+" "+key.filepath+" " +key.label+" "+key.xcoord+" "+key.ycoord+ " " +key.tilewidth+ " "+key.tileheight);
		}
	//	input2.persist(StorageLevel.MEMORY_AND_DISK());
		//long count = input2.count();
		
		
		//for(int i = 0; i < count; i+=20)
	//	{
	//		List<Tuple2<NativeKeyType, int[]>> result = input2.collect();
	//		for(Tuple2<NativeKeyType, int[]> r: result)
	//		{
	//			NativeKeyType key = r._1;
	//			int [] pixels = r._2;
	//			BufferedImage img=new BufferedImage(key.tilewidth, key.tileheight, BufferedImage.TYPE_INT_ARGB);
	//			System.out.println(key.tileID+" "+key.filepath+" " +key.label+" "+key.xcoord+" "+key.ycoord+ " " +key.tilewidth+ " "+key.tileheight);
	//			img.setRGB(0, 0, r._1.tilewidth, r._1.tileheight, pixels, 0, r._1.tilewidth);
	//			ImageIO.write(img, "png", new File("/Users/eyildirim/Documents/output2/"+Math.random()+key.tileID+".png"));
//
	//		}
			
			
	//	}
		

		
		sc.close();	
	}


}
