package edu.rutgers.rdi2.yarn_distributed_transfer.svs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class SVSValue implements Writable {
	
	public BytesWritable RGBBytes = new BytesWritable();
	
	public SVSValue(){
		
	}
    public SVSValue(byte [] bytes){
		RGBBytes.set(bytes, 0, bytes.length);
	}
	public void write(DataOutput out) throws IOException {
        this.RGBBytes.write(out);
    }
	
	public void readFields(DataInput in) throws IOException {
        this.RGBBytes.readFields(in);       
        
    }
}
