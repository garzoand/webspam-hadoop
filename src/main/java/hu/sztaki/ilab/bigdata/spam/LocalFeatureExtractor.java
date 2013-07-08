package hu.sztaki.ilab.bigdata.spam;

import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.spam.enums.FeatureNames;
import hu.sztaki.ilab.bigdata.spam.features.content.BasicContentFeatureCalculator;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 *
 * @author garzo
 */
public class LocalFeatureExtractor {
    
    private static ContentProcessorStack stack = new ContentProcessorStack();
    
    public static void printUsage() {
        System.out.println("LocalFeatureExtractor [html file] [url] [extractors] ...");
        System.out.println("\nExtractors:");
        System.out.println("  basic\n");               
    }
    
    public static void main(String args[]) {
        if (args.length < 3) {
            printUsage();
            return;
        }
        
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("basic")) {                
                stack.addContentProcessor(new BasicContentFeatureCalculator());
                System.out.println("Basic content feature processor added.");
            } else {
                System.out.println("Illegal extractor name: " + args[i]);
                return;
            }
        }
        
        StringBuilder builder = new StringBuilder();
        String line = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(args[0]));
            String ls = System.getProperty("line.separator");
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(ls);
            }            
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return;
        }
        
        HomePageContentRecord record = new HomePageContentRecord();
        HomePageMetaData meta = new HomePageMetaData.Builder(args[1], "hostname")
                .build();
        record.setMetaData(meta);
        record.setContent(builder.toString().getBytes());
        
        stack.execute(record);
        for (Integer featureID : stack.getCalculatedFeatures().keySet()) {
            System.out.println(FeatureNames.getFeatureName(featureID) + " : " + 
                    stack.getCalculatedFeatures().get(featureID));
        }
    }
    
}
