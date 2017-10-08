package edu.rutgers.rdi2.dl4j;

import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;

import org.nd4j.Nd4jSerializer;
import org.nd4j.linalg.cpu.nativecpu.NDArray;

public class TestRegistrator implements KryoRegistrator {
   // @Override
    public void registerClasses(Kryo kryo) {
//        kryo.register(INDArray.class, new Nd4jSerializer());    //DOESN'T WORK - getting same NPE problem
        kryo.register(org.nd4j.linalg.cpu.nativecpu.NDArray.class, new Nd4jSerializer());   //DOES work, but is backend specific
    }
}