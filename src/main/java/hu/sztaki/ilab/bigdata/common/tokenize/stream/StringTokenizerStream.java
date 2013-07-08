/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tokenize.stream;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.strategy.ICUBasedTokenizingStrategy;
import hu.sztaki.ilab.bigdata.common.tokenize.strategy.ITokenizingStrategy;

/**
 *
 * @author garzo
 */
public final class StringTokenizerStream implements ITokenStream {

    private final ITokenizingStrategy tokenizer;
    private String input;
    private Token token = new Token();

    public StringTokenizerStream() {
        this(new ICUBasedTokenizingStrategy());
    }

    public StringTokenizerStream(String input) {
        this();
        this.input = input;
        reset();
    }

    public StringTokenizerStream(ITokenizingStrategy strategy) {
        this.tokenizer = strategy;
    }

    public StringTokenizerStream(ITokenizingStrategy strategy, String input) {
        this.tokenizer = strategy;
        this.input = input;
        reset();
    }

    public void setInput(String input) {
        this.input = input;
        reset();
    }

    public void reset() {
        tokenizer.setText(input);
    }

    public Token next() {
        if (tokenizer.nextToken(token)) {
            return token;
        } else {
            return null;
        }        
    }

    public void close() {
        
    }

}
