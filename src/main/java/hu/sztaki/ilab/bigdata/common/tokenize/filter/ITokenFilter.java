/**
 * ITokenFilter.java
 * Interface for classes which wraps another TokenStream.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */

package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;

/**
 *
 * @author garzo
 */
public interface ITokenFilter extends ITokenStream {

    public void setInput(ITokenStream input);

}
