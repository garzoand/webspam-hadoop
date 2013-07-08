/**
 * PredictionMapper.java
 * Calls classifier to predict spamicity based on precalculated features.
 * Currently supports SVM Model only.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.map;

import hu.sztaki.ilab.bigdata.common.record.FeatureRecord;
import hu.sztaki.ilab.bigdata.ml.svm.SVMModel;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author garzo
 */
public class PredictionMapper extends Mapper<Text, FeatureRecord, Text, DoubleWritable> {
    
    private static final Log LOG = LogFactory.getLog(PredictionMapper.class);
    private static final int TERMS_NUM = 10000;
    
    private SVMModel model = new SVMModel();
    private DoubleWritable dw = new DoubleWritable();
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        try {
            String modelFile = new Path(conf.get(SpamConfigNames.CONF_CLASSIFIER_MODELFILE, 
                    SpamConfigNames.CONF_CLASSIFIER_MODELFILE)).getName();
            Path[] cachedFiles = DistributedCache.getLocalCacheFiles(conf);
            InputStreamReader streamReader = null;
            for (Path cachedFile : cachedFiles) {
                String fileName = cachedFile.getName();
                if (fileName.contains(modelFile)) {
                    model.AddSVM(new FileReader(cachedFile.toString()));
                    break;
                }
            }            
        } catch (Exception ex) {
            Logger.getLogger(PredictionMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    protected void map(Text key, FeatureRecord record, Context context) 
            throws IOException, InterruptedException {
        
        double[] vect = new double[TERMS_NUM];
        Map<Integer, Double> features = record.getFeatures();
        for (int idx : features.keySet()) {                                    
            vect[idx] = features.get(idx);
        }
        model.calculateKernel(vect, 0);
        double prediction = model.predict(0, 1);                
        dw.set(prediction);
        context.write(key, dw);                
    }
}
