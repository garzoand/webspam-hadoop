/**
 * FeatureRecord.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

/**
 *
 * @author garzo
 */
public class FeatureRecord implements Writable {

    private Map<Integer, Double> features;

    public FeatureRecord() {
        features = new TreeMap<Integer, Double>();
    }

    public Map<Integer, Double> getFeatures() {
        return this.features;
    }

    public void addFeature(Integer feature, double value) {
        features.put(feature, value);
    }

    public void setFeatures(Map<Integer, Double> features) {
        this.features = features;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(features.size());
        for (Integer key : features.keySet()) {
            out.writeInt(key);
            out.writeDouble(features.get(key));
        }        
    }

    public void readFields(DataInput in) throws IOException {
        features = new TreeMap<Integer, Double>();
        long size = in.readLong();
        for (long i = 0; i < size; i++) {
            int key = in.readInt();
            double value = in.readDouble();
            features.put(key, value);
        }
    }

}
