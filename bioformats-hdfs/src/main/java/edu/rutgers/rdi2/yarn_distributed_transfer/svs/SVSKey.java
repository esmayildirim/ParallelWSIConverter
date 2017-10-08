package edu.rutgers.rdi2.yarn_distributed_transfer.svs;

import java.io.*;

import org.apache.hadoop.io.*;

public class SVSKey implements WritableComparable<SVSKey>{
	public Text svsFileName = new Text();
	public LongWritable tileID = new LongWritable();
	public LongWritable xcoord = new LongWritable();
	public LongWritable ycoord = new LongWritable();
	public IntWritable tileWidth = new IntWritable();
	public IntWritable tileHeight = new IntWritable();
	public IntWritable label = new IntWritable(0);
	public SVSKey(){
		
	}
	public void write(DataOutput out) throws IOException {
        this.svsFileName.write(out);
        this.tileID.write(out);
        this.xcoord.write(out);
        this.ycoord.write(out);
        this.tileWidth.write(out);
        this.tileHeight.write(out);
        this.label.write(out);
    }
	
	public void readFields(DataInput in) throws IOException {
        this.svsFileName.readFields(in);
        this.tileID.readFields(in);
        this.xcoord.readFields(in);
        this.ycoord.readFields(in);
        this.tileWidth.readFields(in);
        this.tileHeight.readFields(in);
        this.label.readFields(in);
    }

	//Sort ascending by filename, tileID, xcoord, ycoord
   
    public int compareTo(SVSKey second) {
    	
        if(!this.svsFileName.equals(second.svsFileName))
        	return this.svsFileName.compareTo(second.svsFileName);
        else if(this.tileID.get() != second.tileID.get()) 
        	return this.tileID.compareTo(second.tileID);
        else if(this.xcoord.get() != second.xcoord.get())
        	return this.xcoord.compareTo(second.xcoord);
        else if(this.ycoord.get() != second.ycoord.get())
        	return this.ycoord.compareTo(second.ycoord);
        else return this.label.compareTo(second.label);
        
    }
    //Check if two keys are equal
    public boolean equals(Object o) {
        if (!(o instanceof SVSKey)) {
            return false;
        }
        SVSKey other = (SVSKey)o;
        return (this.svsFileName.equals(other.svsFileName) &&
                  this.tileID.get() == other.tileID.get()  &&
                  this.xcoord.get() == other.xcoord.get()  &&
                  this.xcoord.get() == other.xcoord.get()  &&
                  this.tileWidth.get() == other.tileWidth.get()  &&
                  this.tileHeight.get() == other.tileHeight.get() &&
        		  this.label.get() == other.label.get());
    }   
    // hash function to be used by the Hash Partitioner
    public int hashCode() {
        return this.tileID.hashCode() ;
    }
}
