package edu.rutgers.rdi2.bioformats_hdfs.mrjob;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable{
    public IntArrayWritable()
    {
    	super(IntWritable.class);
    	
    }
    public IntArrayWritable(IntWritable [] integers) {
        super(IntWritable.class);
       IntWritable[] ints = new IntWritable[integers.length];
        for (int i = 0; i < integers.length; i++) {
            ints[i] = integers[i];
        }
        set(ints);
    }
}
