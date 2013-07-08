/**
 * StopWordFilter.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author garzo
 */
public class StopWordFilter extends IdentityTokenFilter {

    private DictionaryTrie trie = new DictionaryTrie();
    private static final Log LOG = LogFactory.getLog(StopWordFilter.class);

    public StopWordFilter() {
        
    }
    
    public StopWordFilter(ITokenStream stream) {
        this.inputStream = stream;
    }
    
    public void setStream(ITokenStream stream) {
        this.inputStream = stream;
    }
    
    public void loadFromString(String words) {
        processLine(words);
    }

    public void loadFromFile(String fileName) throws FileNotFoundException, IOException {
        BufferedReader input = null;
        try {
            input = new BufferedReader(new FileReader(fileName));
            String line = null;
            int counter = 0;
            while ((line = input.readLine()) != null) {
                counter += processLine(line);
            }
            LOG.info(counter + " stop words read.");
        } catch (Exception ex) {
            LOG.error("Exception while reading stop word file: " + ex.getMessage());
        } finally {
            if (input != null) {
                input.close();
            }
        }
    }

    private int processLine(String line) {
        PorterStemmerFilter stream = new PorterStemmerFilter(new LowerCaseFilter(
                new WordFilter(new StringTokenizerStream(line))));
        Token token = null;
        int counter = 0;
        while ((token = stream.next()) != null) {
            trie.insert(token.getValue(), 0);
            counter++;
        }
        return counter;
    }

    @Override
    public Token next() {
        if (inputStream != null) {
            Token token = null;
            do {
                token = inputStream.next();
            } while (token != null && trie.contains(token.getValue()));
            return token;
        } else {
            return null;
        }
    }


}
