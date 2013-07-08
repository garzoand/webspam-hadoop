package hu.sztaki.ilab.bigdata.spam.test;

import hu.sztaki.ilab.bigdata.ml.svm.SVMModel;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 * @author garzo
 */
public class SVMTester {
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Parameter needed!");
            return;
        }
        
        try {
            InputStream stream = new FileInputStream(args[0]);
            InputStreamReader reader = new InputStreamReader(stream);
            SVMModel model = new SVMModel();
            System.out.println("Reading model file ...");                       
            model.AddSVM(reader);                    
            
            double[] features = new double[10000];
            features[0] = 5.8972;
            features[1] = 0.1;
            features[2] = 0;
            features[3] = 0;
            features[4] = 5.8972;
            features[5] = 3.2972;
            features[6] = 2.8972;
            features[7] = 5.8972;
            features[8] = 0.8972;
            features[9] = 10.8972;
            features[10] = 6.8972;
            features[11] = 0.5972;
            features[12] = 4.8972;
            features[13] = 2.8972;
            features[14] = 1300.8972;
            features[15] = 348.8972;
            
            model.calculateKernel(features, 0);
            double prediction = model.predict(0, 1);
            System.out.println("Prediction: " + prediction);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            ex.printStackTrace();
        }        
    }       
    
}
