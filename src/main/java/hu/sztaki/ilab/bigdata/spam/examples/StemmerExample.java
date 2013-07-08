package hu.sztaki.ilab.bigdata.spam.examples;

import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 *
 * @author garzo
 */
public class StemmerExample {
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: StemmerExample <sample file>");
            return;
        }
        
        StringBuilder builder = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(args[0]));
            String line = null;
            while ((line = br.readLine()) != null) {                
                builder.append(line).append(" ");
            }
            br.close();                                   
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            return;
        }
        
        ITokenStream stream = new PorterStemmerFilter(new WordFilter(
                new StringTokenizerStream(builder.toString())));
        Token token = null;
        while ((token = stream.next()) != null) {
            System.out.println(token.getValue());
        }
        
    }
    
}
