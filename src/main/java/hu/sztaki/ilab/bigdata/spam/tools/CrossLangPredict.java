/**
 * CrossLangPredict.java
 * Classifies HTML file with cross-language BM25 features and SVM classification.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.tools;

import hu.sztaki.ilab.bigdata.common.dictionary.MultiLangDictionary;
import hu.sztaki.ilab.bigdata.common.job.CmdOptHelper;
import hu.sztaki.ilab.bigdata.common.job.Parameter;
import hu.sztaki.ilab.bigdata.common.parser.IContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.stemming.SnowballStemmerFilter;
import hu.sztaki.ilab.bigdata.ml.svm.OneSVM;
import hu.sztaki.ilab.bigdata.ml.svm.SVMModel;
import hu.sztaki.ilab.bigdata.spam.features.tfreq.TranslatedTermFreqCalculator;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;
import hu.sztaki.ilab.bigdata.spam.termweighting.BM25TermWeighting;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.io.BytesWritable;

/**
 *
 * @author garzo
 */
public class CrossLangPredict {
    
    // based on ClueWeb09 English data set
    public static final long COLLECTION_SIZE = 338378282747L;
    public static final long AVG_DOC_SIZE = 17598;

    private MultiLangDictionary multiLangDictionary = null;
    private SVMModel model = null;
    private HomePageContentRecord record = null;
    private ContentProcessorStack stack = null;
    private boolean printFeatures = false;
    private boolean doPrediction = true;
    private BM25TermWeighting termWeighting = null;
    
    private void init(String[] args) {
        CmdOptHelper options = null;
        try {
            options = CmdOptHelper.create("CrossLangPredict")
                    .setDescription("Classifies a TOKENIZED content with cross-lang BM25 based classification.")
                    .setCopyright("(C) 2012 MTA SZTAKI")
                    .addParameter(Parameter.MODEL_FILE, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter(Parameter.LANGUAGE, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.MULTILANG_DICTIONARY, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.INPUT_FILE, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.PARSER, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter("printfeatures", "Print features with TF/DF", false, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter("nopredict", "Do not calculate prediction but the features", false, CmdOptHelper.ParameterType.NOT_REQUIRED)                    
                    .ParseOptions(args);                
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }                    
        initTermWeighter();
        initDictionary(options.getOptionValue(Parameter.MULTILANG_DICTIONARY),
                       options.getOptionValue(Parameter.LANGUAGE));
        if (options.hasOption("model")) {
            initModel(options.getOptionValue(Parameter.MODEL_FILE));
        } else if (!options.hasOption("nopredict")) {
            System.out.println("No model file given.");
            System.exit(1);
        }
        initHomePageContentRecord(options.getOptionValue(Parameter.INPUT_FILE));
        if (options.hasOption("printfeatures")) printFeatures = true;
        if (options.hasOption("nopredict")) doPrediction = false;
        if (options.hasOption(Parameter.PARSER)) {
            initProcessorStack(options.getOptionValue(Parameter.PARSER), options.getOptionValue(Parameter.LANGUAGE));
        } else {
            initProcessorStack(Parameter.PARSER.defaultValue(), options.getOptionValue(Parameter.LANGUAGE));
        }
    }
    
    private void initDictionary(String fileName, String language) {
        try {
            System.out.println("Reading multi lang dictionary. Language: " + language);
            InputStreamReader stream = new InputStreamReader(
                    new FileInputStream(fileName));
            multiLangDictionary = new MultiLangDictionary(null, 
                    new SnowballStemmerFilter(language));
            multiLangDictionary.setSeparator(" ");
            multiLangDictionary.readFromFile(stream);            
            System.out.println("Dictionary loaded. Num of words: " + multiLangDictionary.getWordsNum());
        } catch (Exception ex) {
            System.out.println("Exception while reading dictionary: " + ex.getMessage());
            System.exit(1);
        }
    }
    
    private void initTermWeighter() {
        termWeighting = new BM25TermWeighting();
        termWeighting.setCollectionSize(COLLECTION_SIZE);
        termWeighting.setAvgDocumentSize(AVG_DOC_SIZE);             
    }    
    
    private void initModel(String fileName) {
        try {
            System.out.println("Reading SVM model from file: " + fileName);
            model = SVMModel.readFromFile(fileName);            
        } catch (Exception ex) {
            System.out.println("Error while reading SVM model: " + ex.getMessage());
            System.exit(1);
        }
    }
    
    private void initHomePageContentRecord(String fileName) {
        try {
            /* Reading content */
            File file = new File(fileName);
            FileInputStream fin = new FileInputStream(file);
            byte[] content = new byte[(int)file.length()];
            fin.read(content);
            
            HomePageMetaData meta = new HomePageMetaData.Builder
                    ("http://x.com", "x.com")
                    .path("index.html")
                    .timestamp(System.currentTimeMillis())
                    .build();
            record = new HomePageContentRecord(new BytesWritable(content), meta);            
        } catch (Exception ex) {
            System.out.println("Error while reading content from file: " + fileName);
            System.exit(1);
        }
    }
    
    private void initProcessorStack(String parsingStrategy, String language) {
        try { 
            Class<? extends IContentParsingStrategy> strategy =
                    (Class<? extends IContentParsingStrategy>) Class.forName(parsingStrategy);
            stack = new ContentProcessorStack(strategy.newInstance());
            TranslatedTermFreqCalculator calculator = new TranslatedTermFreqCalculator();
            calculator.setStemmer(new SnowballStemmerFilter(language));
            calculator.setDictionary(multiLangDictionary);
            stack.addContentProcessor(calculator);
        } catch (ClassNotFoundException ex) {
            System.out.println("Unknown parsing strategy: " + parsingStrategy);
            System.exit(1);
        } catch (Exception ex) {
            System.out.println("Exception occured during the instantiaton of the processor stack: " + ex.getMessage());
            System.exit(1);
        }
    }
    
    private String[] calcFeatures() {
        stack.execute(record);
        String[] features = new String[stack.getCalculatedFeatures().size()*2];
        int i = 0;
        for (int featureID : stack.getCalculatedFeatures().keySet()) {
            features[i] = Integer.toString(featureID);
            features[i+1] = Long.toString(
                            Double.doubleToLongBits(stack
                            .getCalculatedFeatures().get(featureID)));
            i+=2;
        }
        return features;
    }
    
    private void makePrediction(String[] features) {
        if (!doPrediction)
            return;
        
        OneSVM svm = model.getSMVs().get(0);
        if (svm == null) {
            System.out.println("ERROR: there is no svm in the model!");
            System.exit(1);
        }
                
        double[] vect = new double[svm.dim];
        long docSize = 0;
        for (int i = 0; i < features.length; i += 2) {
            int id = Integer.valueOf(features[i]);
            double value = Double.longBitsToDouble(Long.parseLong(features[i + 1]));
            if (id < multiLangDictionary.getWordsNum()) {
                vect[id] += value;
            } else if (id == multiLangDictionary.getWordsNum()) {
                docSize += value;
            }
        }
        
        for (int i = 0; i < multiLangDictionary.getWordsNum(); i++) {
            vect[i] = termWeighting.calculate((long)vect[i], 
                    multiLangDictionary.getDocumentFrequency(i), docSize);
        }
        model.calculateKernel(vect, 0);
        System.out.println("Prediction: " + model.predict(0, 1));
    }
    
    private void printFeatures(String[] features) {
        if (!printFeatures)
            return;
        
        // TODO(garzo) : copy-paste code, merge this with predicion calculator
        double[] vect = new double[multiLangDictionary.getWordsNum()];
        long docSize = 0;
        for (int i = 0; i < features.length; i += 2) {
            int id = Integer.valueOf(features[i]);
            double value = Double.longBitsToDouble(Long.parseLong(features[i + 1]));
            
            if (id < multiLangDictionary.getWordsNum()) {
                vect[id] += value;
                System.out.println("TERM:" + multiLangDictionary.getWord(id) + ":"
                        + vect[id] + ":" + multiLangDictionary.getDocumentFrequency(id));
            } else if (id == multiLangDictionary.getWordsNum()) {
                docSize += value;
            }            
        }
    }
    
    public static void main(String[] args) {
        CrossLangPredict predict = new CrossLangPredict();
        predict.init(args);
        String[] features = predict.calcFeatures();
        predict.printFeatures(features);
        predict.makePrediction(features);
    }

}
