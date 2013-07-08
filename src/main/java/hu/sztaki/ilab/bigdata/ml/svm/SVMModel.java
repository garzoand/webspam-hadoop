package hu.sztaki.ilab.bigdata.ml.svm;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author garzo
 */
public class SVMModel {
    
    private int set = 0;
    float norm_coef;
    List<OneSVM> svm_models = new ArrayList<OneSVM>(1);
    private double[][] train_vec = null;
    private double[] ker = null;
    
    public SVMModel() {
        
    }
    
    public List<OneSVM> getSMVs() {
        return this.svm_models;
    }
    
    public static SVMModel readFromFile(String fileName) throws IOException, Exception {
        SVMModel model = new SVMModel();
        InputStreamReader stream = new InputStreamReader(new GZIPInputStream(
                new FileInputStream(fileName)));
        model.AddSVM(stream);
        return model;        
    }
    
    public void AddSVM(InputStreamReader stream) throws IOException, Exception {        
        String line = null;
        BufferedReader br = new BufferedReader(stream);
        OneSVM svm = new OneSVM();
        long line_num = 0;
        
        try {
            // reading header
            while ((line = br.readLine()) != null && !line.equals("SV")) {
                line_num++;
                String[] s = line.split(" ");
                if (s.length < 2) {
                    System.out.println("Incorrect header line: " + line);
                    continue;
                }
                // kernel type
                if (s[0].equals("kernel_type")) {
                    if (s[1].equals("linear")) {
                        svm.type = 1;
                    } else if (s[1].equals("polynomial")) {
                        svm.type = 2;
                    } else {
                        svm.type = 3;
                    }
                    System.out.println("Kernel type: " + s[1] + " type code: " + svm.type);      
                }
                // degree
                else if (s[0].equals("degree")) {
                    svm.degree = Integer.parseInt(s[1]);
                    System.out.println("Degree: " + svm.degree);
                }
                // gamma
                else if (s[0].equals("gamma")) {
                    svm.gamma = Double.parseDouble(s[1]);
                    System.out.println("Gamma: " + svm.gamma);
                }
                // total_sv
                else if (s[0].equals("total_sv")) {
                    svm.num = Integer.parseInt(s[1]);
                    System.out.println("total_sv: " + svm.num);
                }
                // bias
                else if (s[0].equals("rho")) {
                    svm.bias = Double.parseDouble(s[1]);
                    System.out.println("Bias: " + svm.bias);
                }
                // label
                else if (s[0].equals("label")) {
                    svm.sign = Double.parseDouble(s[1]);
                    if (svm.sign != 1)
                        svm.sign = (float) -1.0;
                    System.out.println("Sign: " + svm.sign);
                }
            }

            if (line == null)
                return; 
            // max dim fixed to 10 000
            train_vec = new double[svm.num][10000];
            for (int i = 0; i < svm.num; i++)
                for (int j = 0; j < 10000; j++)
                    train_vec[i][j] = 0;

            int max_id = 0;
            int line_counter = 0;
            while ((line = br.readLine()) != null) {
                line_num++;
                if (line.length() == 0)
                    continue;
                String fields[] = line.split(" ");
                if (fields.length < 3)
                    continue;
                svm.IDs.add(Integer.parseInt(fields[0]));
                svm.we.add(Double.parseDouble(fields[1]));
                for (int i = 2; i < fields.length; i++) {
                    String[] value = fields[i].split(":");
                    if (value.length > 1) {
                        int id = Integer.parseInt(value[0]);
                        float v = Float.parseFloat(value[1]);
                        train_vec[line_counter][id - 1] = v;
                        if (max_id < id)
                            max_id = id;
                    }
                }
                line_counter++;            
            }
            svm.dim = max_id;
            svm.num = line_counter;

            svm_models.add(svm);
            ker = new double[svm.num];
            for (int i = 0; i < svm.num; i++)
                ker[i] = 0.0;
            set = 1;
        } catch (Exception ex) {
            System.out.println("Exception at " + line_num + " line: " + ex);
            ex.printStackTrace();
            throw ex;
        }
    }
    
    public void calculateKernel(double[] vec, int c) {
        int type = svm_models.get(c).type;
        int num_tr = svm_models.get(c).num;
        int dim = svm_models.get(c).dim;
        
        System.out.println("type: " + type);
        System.out.println("num_tr: " + num_tr);
        System.out.println("dim: " + dim);
        
        for (int i = 0; i < num_tr; i++) {
            ker[i] = 0;
            if (type == 1) { // lin
                for (int d = 0; d < dim; d++) {
                    double val = vec[d];
                    ker[i] += train_vec[i][d] * val;
                }
                // System.out.println("K: " + ker[i]);
            }
            else if (type == 2) { // polynomial
                double sum = 0;
                for (int d = 0; d < dim; d++) {
                    double val = vec[d];
                    sum += train_vec[i][d] * val;
                }
                ker[i] = Math.pow(svm_models.get(c).gamma * sum, svm_models.get(c).degree);
            } else { // RBF
                double sum = 0;
                double sig = 1.0;
                for (int d = 0; d < dim; d++) {
                    // TODO !
                }    
            }
            // System.out.println("----------------------------");
        }
    }
    
    public double predict(int c, int th) {
        double sum = 0.0;
        for (int i = 0; i < svm_models.get(c).IDs.size(); i++) {
            sum += svm_models.get(c).we.get(i) * ker[i];
            // System.out.println("S: " + sum);
        }
        sum -= svm_models.get(c).bias;
        // System.out.println("sum: " + sum);
        double exp = Math.exp(-1 * svm_models.get(c).sign * sum);
        // System.out.println("exp:" + exp);
        double prob = 1 / (1 + Math.exp(-1 * svm_models.get(c).sign * sum));
        return prob;
    }
    
}
