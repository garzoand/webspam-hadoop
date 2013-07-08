/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.common.tokenize.stream;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;

/**
 *
 * @author garzo
 */
public interface ITokenStream {

    public void reset();
    public Token next();
    public void close();    

}
