/**
 * FeatureOutputRecord.java
 * This record will be emitted by the AbstractContentProcessorReducer
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 *
 * @author garzo
 */
public class FeatureOutputRecord implements Writable {

    private List<Double> features;
    private List<String> featureNames;
    
    public FeatureOutputRecord() {
        this(50);
    }

    public FeatureOutputRecord(int size) {
        init(size);
    }

    private void init(int size) {
        features = new ArrayList<Double>(size);
        featureNames = new ArrayList<String>(size);
    }

    public List getFeatures() {
        return this.features;
    }

    public List getFeatureNames() {
        return this.featureNames;
    }

    public void addFeature(String featureName, double value) {
        features.add(value);
        featureNames.add(featureName);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(features.size());
        for (int i = 0; i < features.size(); i++)
            out.writeDouble(features.get(i));
        for (int j = 0; j < featureNames.size(); j++)
            out.writeBytes(featureNames.get(j) + "\n");
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        init(size);
        for (int i = 0; i < size; i++) {
            features.add(in.readDouble());
        }
        for (int j = 0; j < size; j++) {
            featureNames.add(in.readLine());
        }
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < features.size(); i++) {
            builder.append(featureNames.get(i))
                    .append(":").append(features.get(i))
                    .append(" ");
        }
        return builder.toString();
    }

}
