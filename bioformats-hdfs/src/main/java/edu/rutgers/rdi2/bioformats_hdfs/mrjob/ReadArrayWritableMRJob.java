package edu.rutgers.rdi2.bioformats_hdfs.mrjob;


import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


import edu.rutgers.rdi2.yarn_distributed_transfer.svs.SVSKey;

public class ReadArrayWritableMRJob {
	private static final Log LOG = LogFactory.getLog(ReadArrayWritableMRJob.class);
	public static class MyMapper extends
	Mapper<SVSKey, IntArrayWritable, Text, LongWritable> {
		public static Logger logger =
				Logger.getLogger(MyMapper.class);
		public void map(SVSKey key, IntArrayWritable value, Context context)
				throws IOException, InterruptedException {

			String w = key.svsFileName +"-"+ key.tileID+"-" + key.xcoord +"-" + key.ycoord+"-"+key.tileWidth +"-" + key.tileHeight;

			Writable [] array = value.get();
			int [] intArray = new int[array.length];
			int item = 0;
			for (Writable writable: array) {                 // iterate
				IntWritable intWritable = (IntWritable)writable;  // cast
				intArray[item++] = intWritable.get();                    // get
				// do your thing with int value
			}


			BufferedImage img=new BufferedImage(key.tileWidth.get(), key.tileHeight.get(), BufferedImage.TYPE_INT_ARGB);

			img.setRGB(0, 0, key.tileWidth.get(), key.tileHeight.get(), intArray, 0, key.tileWidth.get());
			ImageIO.write(img, "png", new File("/Users/eyildirim/Documents/output2/"+ Math.random() +"-"+key.tileID.get()+".png"));

			context.write(new Text(w+" "+ key.tileHeight.get()+" "+key.tileWidth.get()), new LongWritable(intArray.length));
		}
	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(ReadArrayWritableMRJob.class);
		job.setMapperClass(MyMapper.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(0);
		//      job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);

		long end = System.currentTimeMillis();
		System.out.println(end-start+"\t msecs");
		if (status) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}