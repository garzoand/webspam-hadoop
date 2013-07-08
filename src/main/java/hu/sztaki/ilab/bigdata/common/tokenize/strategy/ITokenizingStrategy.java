package hu.sztaki.ilab.bigdata.common.tokenize.strategy;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;

/**
 *
 * @author garzo
 */
public interface ITokenizingStrategy {

    public void setText(String text);
    public boolean nextToken(Token token);

}
