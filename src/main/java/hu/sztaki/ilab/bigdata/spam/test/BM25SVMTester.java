package hu.sztaki.ilab.bigdata.spam.test;

import hu.sztaki.ilab.bigdata.common.job.CmdOptHelper;
import hu.sztaki.ilab.bigdata.common.job.Parameter;
import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import hu.sztaki.ilab.bigdata.ml.svm.SVMModel;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author garzo
 */
public class BM25SVMTester {
    
    private static final int WORD_NUM = 10000;    
    private static final String OUTPUT_VECTOR_FILENAME = "output.txt";
    private CmdOptHelper options = null;
    
    private Configuration conf;
    private SVMModel model = null;
    private String tableName;
    private String colfamName;
    private String hostName;
    private boolean printOutputVector = false;

    public void initOptions(String [] args) throws IOException, Exception {
        
        try {
            options = CmdOptHelper.create("BM25SVMTester")
                    .setDescription("Test application for SVM classifier")
                    .setCopyright("(C) 2012 MTA SZTAKI")
                    .addParameter(Parameter.HBASE_CONF, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter(Parameter.HBASE_TABLE, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.HBASE_COLFAM, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.MODEL_FILE, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter("host", "Host name", true, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter("printvect", "Print output vector", false, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .ParseOptions(args);                
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }            
        
        tableName = options.getOptionValue(Parameter.HBASE_TABLE);
        colfamName = options.getOptionValue(Parameter.HBASE_COLFAM);
        hostName = options.getOptionValue("host");
        model = SVMModel.readFromFile(options.getOptionValue(Parameter.MODEL_FILE));
        printOutputVector = options.hasOption("printvect");
        
        if (options.hasOption(Parameter.HBASE_CONF)) {
            conf = HBaseConfiguration.create();
            conf.addResource(new Path(options.getOptionValue(Parameter.HBASE_CONF)));            
        }
    }
    
    private void print(double[] vect) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(OUTPUT_VECTOR_FILENAME));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < vect.length; i++) {
                if (vect[i] > 0.0) {
                    sb.append(i + 1).append(":").append(vect[i]).append(" ");
                }
            }
            bw.append(sb.toString().trim());
            bw.newLine();
            bw.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            System.exit(1);
        }
    }
    
    private void go() throws IOException {
        HTable htable = null;
        try {        
            htable = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(hostName));
            get.addFamily(Bytes.toBytes(colfamName));
            get.setMaxVersions(1);
            Result result = htable.get(get);
            if (result == null || result.list() == null || result.list().size() < 1) {
                System.out.println("Unable to fetch host: " + hostName);
            } else {
                FeatureOutputRecord record = new FeatureOutputRecord();
                KeyValue kv = result.list().get(0);
                KeyValue.SplitKeyValue split = kv.split();
                DataInputStream stream = new DataInputStream(new ByteArrayInputStream(split.getValue()));
                record.readFields(stream);
                
                // calculates prediction
                double[] vect = new double[WORD_NUM];
                for (int i = 0; i < record.getFeatures().size(); i++) {                    
                    int idx = Integer.parseInt((String)record.getFeatureNames().get(i));
                    vect[idx] = (Double)record.getFeatures().get(i);
                }
                System.out.println("Calculates kernel ...");
                model.calculateKernel(vect, 0);
                System.out.println("Calculates prediction ...");
                double prediction = model.predict(0, 1);                
                System.out.println("\nPrediction: " + Double.toString(prediction));
                
                if (printOutputVector) {
                    print(vect);
                }
            }
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            System.exit(1);
        } finally {
            if (htable != null) {
                htable.close();
            }
        }
    }
    
    public static void main(String[] args) throws IOException, Exception {
        BM25SVMTester tester = new BM25SVMTester();
        tester.initOptions(args);
        tester.go();
    }

}
