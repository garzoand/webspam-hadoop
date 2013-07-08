package hu.sztaki.ilab.bigdata.ml.svm;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author garzo
 */
public class OneSVM {
    
    public List<Integer> IDs = new ArrayList<Integer>();
    public List<Double> we = new ArrayList<Double>();
    public double bias;
    public double threshold;
    public double gamma;
    public int degree;
    public int num;
    public String name;
    public String kernel;
    public int type;
    public int dim;
    public double sign;
        
}
